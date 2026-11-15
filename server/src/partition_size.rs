//! Validated, unit-bearing partition sizes. SI units use 1000 and IEC units use 1024.
use std::{fmt, str::FromStr};

pub const MAX_PARTITION_BYTES: usize = 64 * 1024 * 1024;

/// A positive partition size no greater than [`MAX_PARTITION_BYTES`].
/// The private byte count keeps programmatic configuration within the same bounds as CLI input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionSize {
    bytes: usize,
}

impl PartitionSize {
    pub fn bytes(self) -> usize {
        self.bytes
    }
}

impl Default for PartitionSize {
    fn default() -> Self {
        Self {
            bytes: MAX_PARTITION_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionSizeError;

impl fmt::Display for PartitionSizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "partition size must be a positive integer with unit B, KB, MB, KiB or MiB, at most {MAX_PARTITION_BYTES} B (64 MiB)",
        )
    }
}

impl std::error::Error for PartitionSizeError {}

impl FromStr for PartitionSize {
    type Err = PartitionSizeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let input = input.trim();
        let digits = input.bytes().take_while(u8::is_ascii_digit).count();
        let quantity = input[..digits]
            .parse::<usize>()
            .map_err(|_| PartitionSizeError)?;
        let multiplier = match input[digits..].trim_start().to_ascii_lowercase().as_str() {
            "b" => 1,
            "kb" => 1_000,
            "mb" => 1_000_000,
            "kib" => 1_024,
            "mib" => 1_048_576,
            _ => return Err(PartitionSizeError),
        };
        let bytes = quantity
            .checked_mul(multiplier)
            .filter(|&bytes| bytes > 0 && bytes <= MAX_PARTITION_BYTES)
            .ok_or(PartitionSizeError)?;
        Ok(Self { bytes })
    }
}

impl fmt::Display for PartitionSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} B", self.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_PARTITION_BYTES, PartitionSize};

    #[test]
    fn units_use_si_and_iec_byte_arithmetic() {
        for (input, bytes) in [
            ("1 B", 1),
            ("64MiB", 67_108_864),
            ("67108864B", 67_108_864),
            ("64kb", 64_000),
            ("64 kb", 64_000),
            ("64 Kb", 64_000),
            ("64 KB", 64_000),
            ("64kib", 65_536),
            ("1 MiB", 1_048_576),
            ("2 mB", 2_000_000),
            (" \t61 mIb\n", 63_963_136),
            ("64 MB", 64_000_000),
            ("64000000b", 64_000_000),
        ] {
            let size: PartitionSize = input.parse().unwrap();
            assert_eq!(size.bytes(), bytes, "{input}");
            assert_eq!(size.to_string().parse::<PartitionSize>().unwrap(), size);
        }
        assert_eq!(MAX_PARTITION_BYTES, 67_108_864);
        assert_eq!(PartitionSize::default().bytes(), MAX_PARTITION_BYTES);
        assert_eq!(PartitionSize::default().to_string(), "67108864 B");
    }

    #[test]
    fn quantities_require_integral_positive_bounded_and_complete_input() {
        for input in [
            "",
            "64",
            "0 B",
            "-1 KB",
            "+1 KB",
            "1.0 MB",
            "0.5 MiB",
            "1e3 B",
            "65 MiB",
            "67108865 B",
            "18446744073709551616 B",
            "18446744073709551615 MiB",
            "1 GB",
            "KB",
            "1 K B",
            "1_000 B",
            "1 B garbage",
            "garbage 1 B",
            "1 MB 2 B",
            "1\0B",
        ] {
            assert!(input.parse::<PartitionSize>().is_err(), "{input:?}");
        }
    }
}
