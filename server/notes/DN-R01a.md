# DN-R01a: job and result DTO representation

Implemented the DTO correction against `plans/distributed-contracts.md`, especially its revised Execution transport and validation section. Independent specification re-check and code-quality review passed. That contract supersedes the embedded Get row and replacement vector illustrated in DN-A01. This assignment changes only `server/src/node/job.rs`, `server/tests/node_job.rs` and this note. Existing shared work is preserved. No dependencies, storage, protocol, engine, runtime, configuration, ORM, scripts or plans changed here.

## Representation and API

Initial Get CONTROL contains `source_rows`, `source_payload_bytes` and `found: bool`, with no row. Its subsequent JSON array must contain exactly zero rows for false or one row for true. `validate_get_rows(result, job, rows)` checks the decoded array against the validated job/result, complete shared schema, requested key, source payload accounting and original escaped-array allowance. It returns the serialized array byte count through a bounded counting writer, without allocating a second serialized row buffer or calling the CONTROL encoder. `parse_get_rows(bytes, result, job)` is a bounded in-memory DTO decoder for that array, not a frame reader. It rejects duplicate row fields, positional row arrays, extra/missing columns, nonfinite numbers, integer/float syntax mismatches, excess rows and trailing JSON; shared Scalar decoding preserves finite float syntax and signed zero. Framing must eventually stream/spool or decode incrementally without buffering the maximum allowance.

Initial Prepared CONTROL contains `effect` and required nullable `replacement_count: Option<u32>`, with no metadata vector. Null means unchanged, zero means changed to empty, and a positive value declares exactly that many separate PartMeta records and Parquet streams. `PartMeta::validate(table)`, `parse_part_meta(bytes, table)` and `encode_part_meta(part, table)` retain strict record/range shape, required nullable bounds, positive readable row/payload/file bounds and lowercase SHA256 digest syntax. Each PartMeta record has its own 16 MiB cap.

`PreparationValidator::new(result, job)` checks the initial CONTROL against the job. `push(&part)` accepts one metadata record at a time, enforcing declared count, typed ordered contiguous coverage of the captured input range, checked aggregate rows/payload/encoded bytes, and the 110,000 pre-finalization rows/files bound. A rejected push permanently invalidates the sequence. `finish(self)` is mandatory and rejects truncation or unfinished coverage; overrun fails on push. State holds counters and range bounds, without collecting metadata. Null/zero counts accept no metadata. There is no combined 80 MiB candidate Parquet cap. The coordinator must later apply global 100,000-row, 64 MiB decoded-payload and catalog limits before uploads.

All earlier job shape, duplicate/unknown-field, identity, task, mutation, captured-snapshot, path, source accounting and numeric bounds remain. Task/file/mutation/result/effect tags use strict `{kind,payload}` objects. Job errors remain sanitized bounded code/message records, without a database outcome. The existing shared protocol types, row validation, canonical ObjectKey parsing, KeyRange rules and mutation validation remain the authorities for their respective semantics.

## Independent limits and unfinished work

- Initial job/result CONTROL and each standalone PartMeta are bounded to 16 MiB. Parse helpers check before deserialization; encoders use bounded sinks.
- Get's JSON row array uses `MAX_INTERNAL_ROW_BYTES = 6 * MAX_DECODED_PAYLOAD_BYTES + 2`, independently of CONTROL and the final public 8 MiB response bound.
- Source data retains 100,000 rows and 64 MiB decoded payload. Each replacement has at most 100,000 rows, 64 MiB decoded payload and 64 MiB encoded bytes; legacy input reads retain 80 MiB. Preparation may total 110,000 rows/files before coordinator finalization. Aggregate payload and encoded counters use checked arithmetic without an invented aggregate encoded cap.
- `ResultEnvelope::validate_for`, `parse_result` and `encode_result` validate CONTROL alone. They do not verify that Get rows, metadata or Parquet streams arrived. Future callers must validate the Get array or finish the metadata sequence, then verify actual Parquet lengths/digests and stream termination. Even null/zero/absent payload cases still require correct framing termination.

No hashing, frame/network reader/writer, authentication, spool implementation, dispatch, global reducer, publication or runtime integration is implemented. These remain later assignments, including R01b. Fresh specification recheck and independent quality review remain parent-owned follow-up work.

## Focused evidence

Every Cargo invocation used the existing `server/target`, ran sequentially, and had this exact environment prefix:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

The initial regression changed the old embedded Get test to a 3 MiB NUL string. Its decoded payload is legal, but escaped JSON exceeds 16 MiB. Before the implementation correction:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test node_job internal_get_row_can_exceed_public_response_cap
FAILED: parse_result returned ValidationError at the old test's unwrap.
0 passed; 1 failed; 4 filtered out (0.30s), exit 101.
```

Final commands and results:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test node_job
6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out (2.09s), exit 0, no warnings.

cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test node_job -- -D warnings
Finished dev profile successfully (5.57s), exit 0, no warnings.

rustfmt --edition 2024 --config skip_children=true --check server/src/node/job.rs server/tests/node_job.rs
exit 0.

git diff --check -- server/src/node/job.rs server/tests/node_job.rs server/notes/DN-R01a.md
exit 0. Owned files are untracked in the shared checkout; an additional file-content whitespace check also passed.
```

The existing five grouped tests were adapted and one focused metadata-stream group added. Evidence includes a Get CONTROL below 1 KiB while its 3 MiB NUL row validates above 16 MiB escaped size; 100,000 and 110,000 metadata records accepted incrementally with initial CONTROL below 1 KiB; encoded aggregates above 80 MiB accepted; truncation, overrun, range gaps, unfinished cover, 110,001 rows/files, bad metadata and stale embedded fields rejected. Tests never build a giant metadata vector, serialize an aggregate list or allocate a 384 MiB buffer. An intermediate run reported an unused import, which was removed before final verification.

No full suite, default AWS build, cloud calls, .env reads, delegates, commits, pushes, branches or worktrees were used. Cargo ownership is released on delivery of this correction.
