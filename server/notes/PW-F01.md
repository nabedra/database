# PW-F01: partition cap is now 64 MiB

The partition hard cap and default target are now 64 * 1024 * 1024 =
67,108,864 closed Parquet bytes, including metadata. This supersedes the decimal
64,000,000-byte cap described in earlier PW notes and acceptance results.
Those historical notes and results remain unchanged.

`partition_size::MAX_PARTITION_BYTES` remains the Rust source of truth. The
parser, default, bounded codec sink, production readers, manifest validation,
engine checks and publication checks use it. Diagnostics and current guides
now describe 64 MiB. `64MiB` and `67108864B` succeed; `65 MiB` and `67108865B`
fail. SI/IEC arithmetic is unchanged: `64 MB` still means 64,000,000 bytes,
`64 KB` means 64,000 bytes and `64KiB` means 65,536 bytes.

The 100,000-row and 64 MiB decoded-payload table limits are unchanged, as are
legacy encoded-snapshot limits and schema bounds. The acceptance inspectors
now allow at most 67,108,864 bytes per partition. The live scenario still uses
its configured `64 KB` target and checks files against 64,000 bytes.

The real-codec boundary fixture retains its valid two-column schema. It obtains
accounting overhead from `snapshot::validate_rows` on rows with empty strings
and subtracts that overhead before allocating the large string values. Both
the two-row candidate and singleton account for exactly 64 MiB of decoded
payload. Encoding each candidate with the existing snapshot codec proves that
its closed Parquet file exceeds the partition cap. Partition encoding splits
the two-row candidate into two bounded singleton files; the oversized singleton
returns `LimitExceeded/NotApplied`. No public test API or schema change was needed.
Storage and commit rejection fixtures use `MAX_PARTITION_BYTES + 1`.

Changed files for this follow-up, in addition to this note:

- `server/src/partition_size.rs`, `server/src/config.rs`, `server/src/commit.rs`
- `server/tests/cli.rs`, `server/tests/config.rs`, `server/tests/storage.rs`,
  `server/tests/commit.rs`, `server/tests/snapshot.rs`
- `scripts/acceptance_concurrency.py`, `scripts/acceptance_partitions.py`,
  `scripts/test_acceptance_support.py`
- `README.md`, `server/README.md`, `TUTORIAL.md`, `orm/README.md`

Validation used existing `server/target`, sequential Cargo commands and
`CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`:

- Red: `cargo test --manifest-path server/Cargo.toml --lib accepts_64_mib_as_the_default_hard_cap`
  failed because the old parser rejected `64MiB`.
- Green: `cargo test --manifest-path server/Cargo.toml --lib partition_size::tests`
  passed all 3 parser tests, including the new regression and retained SI cases.
- `cargo test --manifest-path server/Cargo.toml --test cli --test config --test storage --test commit --test snapshot`
  passed all 34 tests: CLI 4, config 4, storage 7, commit 10, snapshot 9.
- `PYTHONPATH=scripts orm/.venv/bin/python -m unittest scripts.test_acceptance_support.CatalogShapeGuard`
  passed its existing catalog guard with the added inclusive-cap boundary cases.
  The first invocation with system `python3` could not import the existing
  future-annotations syntax; the repository ORM environment resolved it.
- `cargo fmt --manifest-path server/Cargo.toml --all` and the corresponding
  `-- --check` passed.
- `cargo clippy --manifest-path server/Cargo.toml --all-targets -- -D warnings`
  passed.
- `cargo build --manifest-path server/Cargo.toml` rebuilt the server successfully.

No full Rust suite or AWS checks were rerun. The previous 53 live checks at the
64 KB target remain historical evidence, not a new-cap live run. No unresolved
implementation concerns were found. Shared earlier changes and
`plans/partition-workers.md` were preserved. No commits or pushes were made.

Independent specification and quality reviews both passed without findings.
The coordinator reconciled the follow-up rubric; the change is complete.
