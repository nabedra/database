# DN-C01b: provider-neutral storage reads and optional S3

Status: DONE_WITH_CONCERNS

## Changes

- `server/src/storage.rs`: `Download.body` is `Pin<Box<dyn tokio::io::AsyncRead + Send>>`; `Download::new` accepts readers without requiring Unpin, and `Download::from_bytes` owns a Vec through a Cursor. Reads use an 8 KiB scratch buffer and probe at most one byte beyond the remaining limit. I/O failures remain Unavailable and excess data remains LimitExceeded. The trait now documents complete linearizable GET, durable atomic Create/Match, non-ABA versions, and conservative Ambiguous outcomes without PUT retries.
- `server/src/storage/s3.rs`: moved existing S3 implementation and `Storage<S3Store>::connect`. Public adapter module and `storage::S3Store` reexport preserve existing imports. Only behavior-level expression changed is `Download::new(response.body.into_async_read(), etag)`. A Python comparison against the initial file confirmed the adapter body otherwise matches exactly.
- `server/Cargo.toml`: optional aws-config/aws-sdk-s3 dependencies behind `s3`; default retains `s3`. `Cargo.lock` is byte-identical to its starting contents.
- `server/src/main.rs`: narrow cfgs retain legacy startup with S3; without S3, serve returns the static sanitized unsupported-configuration error before loading configuration or starting services.
- `server/tests/support/storage.rs`: existing multi-chunk fake now implements Tokio AsyncRead instead of AWS/HTTP bodies. All existing fake conditional semantics remain unchanged.
- `server/tests/storage.rs`: one focused reader check covers a never-ending stream, zero-byte bound, partial-read I/O failure, and from_bytes. Existing tests cover exact-limit reads, conditional races, lost responses, namespace validation, and immutable objects.
- `server/notes/DN-C01b.md`: this handoff.

No lib.rs change was necessary. No catalog/commit/runtime/config/HTTP/Python semantics were edited. Existing uncommitted work was retained. No agents, branches, commits, pushes, cloud services, installs, downloads, or .env reads were used.

## SDK evidence

Inspected installed registry sources, not a guessed API:

- `aws-smithy-types-*/src/byte_stream.rs:434`: public `into_async_read(self) -> impl tokio::io::AsyncBufRead`, gated by rt-tokio, implemented with tokio_util StreamReader.
- `aws-sdk-s3-1.158.0/Cargo.toml:42-64`: existing default features include rt-tokio and activate aws-smithy-types/rt-tokio. Default-feature compilation confirms the returned reader meets Send and ownership bounds.

## Validation

All Cargo commands ran sequentially in `server`, using the existing `server/target`, with `CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`. `--offline` prevents downloads. No full integration or acceptance suites were launched.

1. `cargo test --locked --offline --test storage neutral_reader_bounds_and_partial_failure` before implementation: exit 101, expected missing Download::new/from_bytes compiler errors. An earlier invocation before the test was written ran zero tests because a setup script used the wrong cwd; it is not counted as evidence.
2. `cargo test --locked --offline --test storage --test commit`: PASS, 8 storage tests and 10 commit tests. Reader evidence checks consumption stops at 18 bytes for limit 17, at one byte for limit zero, and maps failure after eight bytes to Unavailable.
3. `cargo check --locked --offline --no-default-features --lib --bin meshdb-server`: PASS.
4. `cargo clippy --locked --offline --lib --bin meshdb-server --test storage --test commit -- -D warnings`: PASS.
5. `cargo clippy --locked --offline --no-default-features --lib --bin meshdb-server -- -D warnings`: PASS.
6. `rustfmt --edition 2024 --config skip_children=true --check server/src/storage.rs server/src/storage/s3.rs server/src/main.rs server/tests/storage.rs server/tests/support/storage.rs` from repository root: PASS. Same owned-file invocation without --check applied formatting.
7. `git diff --check -- server/Cargo.toml server/Cargo.lock server/src/storage.rs server/src/storage/s3.rs server/src/main.rs server/tests/storage.rs server/tests/support/storage.rs`: PASS.

8. `cargo run --locked --offline --no-default-features --bin meshdb-server -- serve`: PASS through a Python subprocess assertion wrapper. Exit 1, empty stdout, final stderr line exactly `meshdb-server: unsupported configuration: legacy serve requires the s3 feature`. Built and ran the actual binary without credentials or storage access.
9. `cargo tree --locked --offline --no-default-features --edges normal`: PASS through a Python assertion wrapper checking that output contains neither `aws-sdk-` nor `aws-config `.

## Compatibility and concerns

The source-level Download.body type intentionally changes. Backend constructors should use Download::new(reader, etag) or Download::from_bytes(bytes, etag). Existing Storage<S3Store>::connect and storage::S3Store paths remain available with default features.

Legacy S3 keeps ETag conditional semantics exactly. S3 ETags are not generally non-ABA generation tokens for arbitrary repeated payloads, so this adapter does not establish the stronger backend contract for arbitrary overwrite histories. Existing catalog writes include fresh commit UUIDs; this bounded task does not add versioning or change publication. A future distributed S3 conformance review must resolve that gap. No live S3 behavior was exercised.

The no-default-features build omits the AWS SDK and AWS configuration dependencies. Rustls may still use the aws-lc-rs cryptographic library, which is unrelated to S3 clients or credentials. Full non-S3 HTTP startup remains later work.

The last executable build is the no-S3 `server/target/debug/meshdb-server`. Parent must rebuild with default features before reusing that binary for legacy S3 startup. Remaining disk after checks is approximately 945 MiB; no unrelated build files were removed.

## Independent review

Specification review passed after inspecting neutral reader bounds/errors,
feature gates and compatibility exports, sanitized unsupported startup, unchanged
fake conditional semantics and the focused test evidence. No correction was
requested and no Cargo/test commands were repeated. Independent quality review
also passed without critical or important findings after checking ownership,
bounded streaming, conditional writes/retries, feature compatibility and fake
reader behavior. The recorded build/test evidence was reused.
