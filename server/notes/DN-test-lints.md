# Distributed library and test lint cleanup

The parent closed the broader test-target diagnostics recorded by the earlier
distributed slices. This is test infrastructure and assertion cleanup; it adds
no production behavior or runtime acceptance claim.

## Changes

- Declare the existing `tests/support/storage.rs` once as the library's
  `cfg(test)` `crate::test_storage`. Five library-test modules import that
  declaration instead of compiling the same file repeatedly. The Fake's
  implementation and instance-owned state are unchanged. Integration-test
  crates keep their existing support modules. Future library tests should reuse
  `crate::test_storage`.
- Remove an ineffective `?Sized` bound from the membership negative Clone
  assertion and an unnecessary iterator collection from an authentication test.
- Collapse a test-only pending-submission hook's nested conditional and use
  lexical scopes for two test MutexGuards. Both previous guard sites already
  explicitly dropped the guard before awaiting; this does not claim a newly
  discovered runtime deadlock.
- Replace existing boxed test values in place. In the escaped-Get send test,
  count the actual bytes passed through the chunk iterator to the spool and
  assert that count exceeds the CONTROL limit after both fixtures finish.
  This replaces a constant-only assertion. It measures encoded fixture bytes,
  not peak memory or network traffic.

Owned Rust files:

```text
server/src/lib.rs
server/src/cluster/store/tests.rs
server/src/cluster/attachment/tests.rs
server/src/cluster/store/admin/tests.rs
server/src/cluster/admin_history/tests.rs
server/src/node/payload_owner/local_tests.rs
server/src/cluster/membership/tests.rs
server/src/node/auth/verify/content/tests.rs
server/src/node/payload_owner.rs
server/src/node/payload_owner/tests.rs
server/src/node/payload_stream/send_tests.rs
```

The parent also writes this note and updates the rubric's progress log. Shared
uncommitted work is preserved. No dependency, manifest, lockfile, production
protocol or public API change is included.

## Personally executed verification

Every Cargo command used:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

The initial broad check failed with eight diagnostics: the six previously
recorded findings and two newer test-only findings. No lint suppressions were
added. After correction, this command passed in 24.29s:

```sh
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

The affected tests then passed through the following sequential loop, which
exits on its first failed Cargo command. It returned exit status 0. The eight
filters cover 57 tests: 14 store, 7 administration publication, 6 history,
6 attachment, 16 payload owner/local composition, 6 send and 2 focused tests.

```sh
for meshdb_test_filter in \
  cluster::store::tests \
  cluster::store::admin::tests \
  cluster::admin_history::tests \
  cluster::attachment::tests \
  node::payload_owner:: \
  node::payload_stream::send_tests \
  node::auth::verify::content::tests::exact_request_hash_claims_and_invocation_association \
  cluster::membership::tests::consumed_parts_diagnostics_and_retained_stop_evidence
do
  CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 \
    cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features \
      --lib "$meshdb_test_filter" -- --test-threads=2 || exit $?
done
```

Owned `rustfmt --check --edition 2024 --config skip_children=true` and direct
trailing-whitespace/final-newline checks passed, including untracked files.
These are affected component tests, not a complete suite or default/S3-feature
build. The behavioral tests were reused and strengthened where needed; no
failing-before behavioral claim is made for these lint corrections.

Cargo/build and Rust implementation ownership is released. Earlier notes retain
their historical lint results; this note supersedes their pending test-lint
status for the current no-default library/test tree. Network, authority,
startup and distributed acceptance remain separate unfinished work.
