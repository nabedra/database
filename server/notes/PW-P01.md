# PW-P01: partition size parsing and options

Added `partition_size::PartitionSize`, a Copy type with private bytes, a `bytes()` accessor,
`FromStr`, and byte-unit `Display`. Its default and `MAX_PARTITION_BYTES` are 64,000,000.
Parsing requires positive ASCII integer quantities and a case-insensitive B/KB/MB/KiB/MiB
unit. SI uses 1000 and IEC uses 1024. Outer whitespace and whitespace before the unit
are allowed; the entire input must match. Checked arithmetic rejects overflow and sizes
above the maximum. Thus 64 MiB fails and 61 MiB succeeds.

Decimal-input policy is deliberate: reject all decimal notation, including `1.0 MB`,
even when it would denote an integral byte count. Bare numbers, signs, exponents,
unknown units and trailing garbage also fail.

`ServeOptions.partition_size` uses this validated type, so safe programmatic construction
preserves its bounds. Clap exposes `--partition-size` and visible alias `--partition_size`,
with the type's default. Worker defaults and explicit worker values remain independent.
No runtime, engine or storage behavior changed; no dependencies were added.

## Evidence

All Cargo commands ran sequentially from `server/`, using the existing `server/target`
with `CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`.

- Red: `cargo test --test cli partition_size_spellings_allow_default_and_explicit_workers`
  failed with `UnknownArgument` for `--partition-size` before implementation.
- Red: `cargo test --lib partition_size` failed to compile the new parser tests because
  `PartitionSize` and `MAX_PARTITION_BYTES` did not yet exist.
- Green: `cargo test --lib partition_size` passed both behavior groups. They cover
  units, byte arithmetic, Display round trips, defaults, bounds and invalid inputs.
- Green: `cargo test --test cli --test config` passed all 8 tests, including both CLI
  spellings with default and explicit workers, visible help alias, invalid CLI values,
  and default/explicit sizes retained through Config.
- `rustfmt --edition 2024 --config skip_children=true src/partition_size.rs src/lib.rs src/config.rs tests/config.rs tests/cli.rs`
  and the same command with `--check` passed. `git diff --check` passed.

Files: `server/src/partition_size.rs`, `server/src/lib.rs`, `server/src/config.rs`,
`server/tests/config.rs`, `server/tests/cli.rs`, and this note. No additional exhaustive
ServeOptions literal edits were needed. `server/src/cli.rs` already delegates to
ServeOptions and needed no edit.

No unresolved concerns within this assignment. Full suites and Clippy were not run,
as requested. No AWS calls were made. The coordinator-owned plan was left untouched.
