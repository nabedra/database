//! CRC32C (Castagnoli, reflected polynomial `0x82F63B78`).
//!
//! The implementation is chosen once per process: SSE4.2 `crc32` on x86_64, the ARMv8 CRC
//! extension on aarch64, otherwise a slicing-by-8 table walk whose tables are computed at
//! compile time. Every path computes the same function; the tests check them against a
//! bitwise reference.

use std::sync::atomic::{AtomicU8, Ordering};

/// CRC32C of `data`.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    extend(0, data)
}

/// Continues a CRC32C: `extend(crc32c(a), b) == crc32c(a ‖ b)`.
#[inline]
pub fn extend(crc: u32, data: &[u8]) -> u32 {
    match implementation() {
        // SAFETY: `implementation()` returns `Hardware` only after `hardware::available()`
        // reported the CPU feature.
        Implementation::Hardware => unsafe { hardware::extend(crc, data) },
        Implementation::Software => software_extend(crc, data),
    }
}

const MASK_DELTA: u32 = 0xA282_EAD8;

/// Masks a CRC before it is stored next to the data it covers, so a CRC of bytes that
/// themselves contain CRCs stays well distributed (the LevelDB convention).
pub fn mask(crc: u32) -> u32 {
    crc.rotate_right(15).wrapping_add(MASK_DELTA)
}

/// Inverse of [`mask`].
pub fn unmask(masked: u32) -> u32 {
    masked.wrapping_sub(MASK_DELTA).rotate_left(15)
}

/// Which code path [`extend`] uses on this machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Implementation {
    /// SSE4.2 on x86_64, the CRC extension on aarch64.
    Hardware,
    /// Slicing-by-8 tables.
    Software,
}

const UNKNOWN: u8 = 0;
const HARDWARE: u8 = 1;
const SOFTWARE: u8 = 2;

static IMPLEMENTATION: AtomicU8 = AtomicU8::new(UNKNOWN);

/// The path chosen for this process (detected on first use, then cached).
#[inline]
pub fn implementation() -> Implementation {
    match IMPLEMENTATION.load(Ordering::Relaxed) {
        HARDWARE => Implementation::Hardware,
        SOFTWARE => Implementation::Software,
        _ => detect(),
    }
}

#[cold]
fn detect() -> Implementation {
    // Detection is idempotent, so racing threads store the same value.
    let (code, found) = if hardware::available() {
        (HARDWARE, Implementation::Hardware)
    } else {
        (SOFTWARE, Implementation::Software)
    };
    IMPLEMENTATION.store(code, Ordering::Relaxed);
    found
}

const POLY: u32 = 0x82F6_3B78;

/// `TABLES[0]` is the byte-at-a-time table; `TABLES[k][b]` is the CRC of byte `b`
/// followed by `k` zero bytes.
static TABLES: [[u32; 256]; 8] = make_tables();

const fn make_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0u32; 256]; 8];
    let mut b = 0;
    while b < 256 {
        let mut c = b as u32;
        let mut bit = 0;
        while bit < 8 {
            c = if c & 1 != 0 { (c >> 1) ^ POLY } else { c >> 1 };
            bit += 1;
        }
        tables[0][b] = c;
        b += 1;
    }
    let mut k = 1;
    while k < 8 {
        let mut b = 0;
        while b < 256 {
            let prev = tables[k - 1][b];
            tables[k][b] = (prev >> 8) ^ tables[0][(prev & 0xFF) as usize];
            b += 1;
        }
        k += 1;
    }
    tables
}

/// Slicing-by-8: eight table lookups per 8 input bytes, no data-dependent branches.
pub(crate) fn software_extend(crc: u32, data: &[u8]) -> u32 {
    let t = &TABLES;
    let mut c = !crc;
    let (words, tail) = data.as_chunks::<8>();
    for chunk in words {
        let lo = c ^ u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        let hi = u32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        c = t[7][(lo & 0xFF) as usize]
            ^ t[6][((lo >> 8) & 0xFF) as usize]
            ^ t[5][((lo >> 16) & 0xFF) as usize]
            ^ t[4][(lo >> 24) as usize]
            ^ t[3][(hi & 0xFF) as usize]
            ^ t[2][((hi >> 8) & 0xFF) as usize]
            ^ t[1][((hi >> 16) & 0xFF) as usize]
            ^ t[0][(hi >> 24) as usize];
    }
    for &b in tail {
        c = (c >> 8) ^ t[0][((c ^ u32::from(b)) & 0xFF) as usize];
    }
    !c
}

#[cfg(target_arch = "x86_64")]
mod hardware {
    use std::arch::x86_64::{_mm_crc32_u8, _mm_crc32_u64};

    pub fn available() -> bool {
        std::arch::is_x86_feature_detected!("sse4.2")
    }

    /// # Safety
    ///
    /// The CPU must support SSE4.2: call only after [`available`] returned true.
    #[inline]
    pub unsafe fn extend(crc: u32, data: &[u8]) -> u32 {
        // SAFETY: the caller guarantees SSE4.2 (this function's contract).
        unsafe { extend_sse42(crc, data) }
    }

    #[target_feature(enable = "sse4.2")]
    fn extend_sse42(crc: u32, data: &[u8]) -> u32 {
        let mut c = u64::from(!crc);
        let (words, tail) = data.as_chunks::<8>();
        for &chunk in words {
            let word = u64::from_le_bytes(chunk);
            c = _mm_crc32_u64(c, word);
        }
        let mut c = c as u32;
        for &b in tail {
            c = _mm_crc32_u8(c, b);
        }
        !c
    }
}

#[cfg(target_arch = "aarch64")]
mod hardware {
    use std::arch::aarch64::{__crc32cb, __crc32cd};

    pub fn available() -> bool {
        std::arch::is_aarch64_feature_detected!("crc")
    }

    /// # Safety
    ///
    /// The CPU must have the CRC extension: call only after [`available`] returned true.
    #[inline]
    pub unsafe fn extend(crc: u32, data: &[u8]) -> u32 {
        // SAFETY: the caller guarantees the CRC extension (this function's contract).
        unsafe { extend_crc(crc, data) }
    }

    #[target_feature(enable = "crc")]
    fn extend_crc(crc: u32, data: &[u8]) -> u32 {
        let mut c = !crc;
        let (words, tail) = data.as_chunks::<8>();
        for &chunk in words {
            let word = u64::from_le_bytes(chunk);
            c = __crc32cd(c, word);
        }
        for &b in tail {
            c = __crc32cb(c, b);
        }
        !c
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
mod hardware {
    pub fn available() -> bool {
        false
    }

    /// # Safety
    ///
    /// Never unsafe here; `unsafe` only to match the other architectures.
    pub unsafe fn extend(crc: u32, data: &[u8]) -> u32 {
        super::software_extend(crc, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bitwise definition every path must match.
    fn reference(crc: u32, data: &[u8]) -> u32 {
        let mut c = !crc;
        for &b in data {
            c ^= u32::from(b);
            for _ in 0..8 {
                c = if c & 1 != 0 { (c >> 1) ^ POLY } else { c >> 1 };
            }
        }
        !c
    }

    type Path = (&'static str, fn(u32, &[u8]) -> u32);

    /// Every path available on this machine, with its name.
    fn paths() -> Vec<Path> {
        let mut paths: Vec<Path> = vec![
            ("reference", reference),
            ("software", software_extend),
            ("dispatch", extend),
        ];
        if hardware::available() {
            paths.push(("hardware", hardware_checked));
        }
        paths
    }

    /// The hardware path, after checking the CPU feature.
    fn hardware_checked(crc: u32, data: &[u8]) -> u32 {
        assert!(hardware::available());
        // SAFETY: the feature was just detected.
        unsafe { hardware::extend(crc, data) }
    }

    struct SplitMix(u64);

    impl SplitMix {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    #[test]
    fn ns_unit_crc32c_known_answers_on_every_path() {
        let ascending: Vec<u8> = (0u8..32).collect();
        let descending: Vec<u8> = (0u8..32).rev().collect();
        // RFC 3720, appendix B.4: a SCSI READ(10) command PDU.
        let scsi_read: [u8; 48] = [
            0x01, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x14,
            0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let vectors: [(&[u8], u32); 7] = [
            (b"", 0),
            (b"123456789", 0xE306_9283),
            (&[0u8; 32], 0x8A91_36AA),
            (&[0xFFu8; 32], 0x62A8_AB43),
            (&ascending, 0x46DD_794E),
            (&descending, 0x113F_DB5C),
            (&scsi_read, 0xD996_3A56),
        ];
        for (name, path) in paths() {
            for (data, expected) in vectors {
                assert_eq!(
                    path(0, data),
                    expected,
                    "{name} path, {} byte vector",
                    data.len()
                );
            }
        }
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(extend(crc32c(b"1234"), b"56789"), crc32c(b"123456789"));
    }

    #[test]
    fn ns_unit_crc32c_mask_round_trips() {
        let mut rng = SplitMix(7);
        for _ in 0..1000 {
            let crc = rng.next() as u32;
            assert_eq!(unmask(mask(crc)), crc);
            assert_ne!(mask(crc), crc);
        }
        assert_eq!(unmask(mask(0xDEAD_BEEF)), 0xDEAD_BEEF);
    }

    #[test]
    fn ns_unit_crc32c_paths_match_reference_on_random_unaligned_input() {
        let mut rng = SplitMix(0x5EED_C3C3);
        let mut buf = vec![0u8; 4096 + 16];
        for b in &mut buf {
            *b = rng.next() as u8;
        }
        let paths = paths();
        for _ in 0..2000 {
            let start = rng.below(16) as usize;
            let len = rng.below(4097) as usize;
            let data = &buf[start..start + len];
            let seed = if rng.below(2) == 0 {
                0
            } else {
                rng.next() as u32
            };
            let expected = reference(seed, data);
            for (name, path) in &paths {
                assert_eq!(path(seed, data), expected, "{name} start={start} len={len}");
            }
            // Splitting the input anywhere gives the same CRC.
            let split = rng.below(len as u64 + 1) as usize;
            assert_eq!(
                extend(extend(seed, &data[..split]), &data[split..]),
                expected
            );
        }
        // Every length 0..=64 at every alignment 0..8, which covers each tail size.
        for start in 0..8 {
            for len in 0..=64 {
                let data = &buf[start..start + len];
                let expected = reference(0, data);
                for (name, path) in &paths {
                    assert_eq!(path(0, data), expected, "{name} start={start} len={len}");
                }
            }
        }
    }

    #[test]
    fn ns_unit_crc32c_detection_is_cached() {
        let first = implementation();
        assert_eq!(implementation(), first);
        assert_ne!(IMPLEMENTATION.load(Ordering::Relaxed), UNKNOWN);
        if cfg!(any(target_arch = "x86_64", target_arch = "aarch64")) {
            assert_eq!(first == Implementation::Hardware, hardware::available());
        }
    }

    /// Throughput of each path. Run with
    /// `cargo test --release -p nabedra-kv crc::tests::ns_unit_crc32c_bench -- --ignored --nocapture`.
    #[test]
    #[ignore = "benchmark"]
    fn ns_unit_crc32c_bench() {
        let mut rng = SplitMix(1);
        let data: Vec<u8> = (0..1 << 20).map(|_| rng.next() as u8).collect();
        for (name, path) in paths() {
            for size in [64usize, 4096, 1 << 20] {
                let total: usize = if name == "reference" {
                    32 << 20
                } else {
                    1 << 30
                };
                let rounds = total / size;
                let mut acc = 0u32;
                let started = nabedra_rt::time::now_std();
                for i in 0..rounds {
                    let offset = (i * 64) % (data.len() - size + 1);
                    acc ^= path(acc, std::hint::black_box(&data[offset..offset + size]));
                }
                let secs = started.elapsed().as_secs_f64();
                std::hint::black_box(acc);
                println!(
                    "crc32c {name:>9} {size:>8} B blocks: {:>8.0} MB/s",
                    (rounds * size) as f64 / secs / 1e6
                );
            }
        }
    }
}
