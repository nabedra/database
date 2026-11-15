# DN-C08c1 Config generation 2 codec and synthetic roots

Implemented the already adopted A11 section 3 amendment only, against the shared
contracts, relevant distributed-nodes rubric, A05/A08, A11 section 4 boundary,
DN-C04a2b and actual codecs. Applied TDD and unslop. No applicable AGENTS.md was
found. Shared tracked and untracked work was preserved. No delegation, branches,
worktrees, commits, pushes, secret/.env reads, network/cloud calls, dependency
changes or fetches occurred. Historical delivery notes and the rubric are unchanged.

## Exact changes

- `server/src/cluster/state.rs` adds required nonnullable String
  `Config.worker_semantic_fingerprint` and includes it in the existing lowercase
  64-hex digest validator. Only generation 2 validates. Root version remains 1.
  There are no other production changes, including capacity arithmetic.
- `server/tests/control_state.rs` uses the generation-2 root fixture with
  deterministic `"d".repeat(64)` worker identity. The existing duplicate-generation
  check now duplicates 2, and the existing unsupported-generation corruption now
  uses 1. Added one focused schema group.
- Only the existing synthetic root line changes in `server/tests/control_store.rs`,
  `server/src/cluster/controller.rs` under cfg(test),
  `server/src/cluster/store/tests.rs`, `server/src/cluster/store/admin/tests.rs`,
  `server/src/cluster/admin/tests.rs` and
  `server/src/cluster/admin_history/tests.rs`. Each uses generation 2 and the
  same deterministic valid worker digest. Direct comparisons with pre-edit
  snapshots confirm those six files have exactly that single-line amendment.
- This new note records the coordinated amendment. A repository Rust search of
  Config constructors, generation fields and storage fingerprints found no
  additional durable Config fixture or constructor requiring ownership expansion.
  Backend version counters and controller authorization generations are unchanged.

The schema group validates a generation-2 decode/roundtrip, constructed root
validate/encode, and standalone Config validate/encode/decode. It rejects
unsupported generations 0, 1, 3 and u64::MAX; generation 1 rejects both with and
without the new identity. Starting from a valid baseline each time, it rejects
missing/null identities, empty/63/65-byte digests, nonhex and uppercase digests,
and a duplicate new field inserted into original JSON bytes. Representable
invalid constructed candidates fail validate and encode too. Existing nested
object/unknown/duplicate checks continue unchanged except their generation token.

## Fixed allowance

No bound change is necessary. The added field plus comma costs 97 compact JSON
bytes. Five valid 64-byte ASCII digests, generation 2 and the longer permitted
adapter value produce a maximum valid compact Config of 510 bytes. These values
need no JSON escaping; the null adapter is shorter. Config's existing 4096-byte
independent encoded cap therefore still covers every valid Config.

The C04a2b fixed proof remains 65536 bytes for Template, 5*4096 for Timing and
four global records, 4096 for other root keys/scalars/brackets and 4096 for array
separators. Its total is still 94208, below the unchanged 262144-byte allowance.
Root validation invokes these individual bounded codecs. Node/intent bounds,
first-stop reservations and all capacity expectations are unchanged.

## Commands and results

All Cargo commands ran sequentially from the repository root, reused existing
`server/target`, and used this exact prefix, with no default/AWS build:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Each following command was invoked with that prefix:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_state generation_two_requires_worker_semantic_identity
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_state
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_store
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::controller::
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::store::tests::
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::store::admin::tests::
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::admin::tests::
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::admin_history::tests::
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
```

The first command compiled successfully against the prior production codec and
failed behaviorally at valid generation-2 decode with InvalidRecord, exit 101.
There were no Rust compile failures. After implementation the seven test commands
passed 10, 9, 20, 14, 7, 8 and 6 tests respectively, 74 total and zero failures.
Test execution times were 2.86s, 1.41s, 3.95s, 0.47s, 0.55s, 1.83s and 3.27s.
Strict production-library Clippy passed in 10.73s. No broader material/auth/payload
suite or test/all-target Clippy was run. The six previously reported unrelated
Fake/payload test diagnostics remain parent cleanup; prior broader evidence is
reused rather than rerun.

Owned Rust files were formatted and checked with `rustfmt --edition 2024
--config skip_children=true` and the same command with `--check`. Scoped
`git diff --check` and direct trailing-whitespace/final-newline checks cover all
nine owned files, including untracked files for which ordinary Git diff has no
baseline. All passed. No fixture behavior expectation required correction beyond
the generation-specific checks described above.

## Readiness limits and handoff

These codecs validate digest representation only. They perform no cryptographic
computation, material reads, attachment comparison, startup I/O, root write,
upgrade, backfill or fallback. Generation 1 cannot decode as valid current
Control, with or without the new field. No missing identity is synthesized.
Template, Timing, adapter contract, other Config fields, public input versions
and control transition behavior are unchanged. The affected existing regression
modules pass with the new Config shape through load/initialization, ownership,
policy, history and publication paths.

C08c2 must compute/compare retained material identities at the adopted attachment
boundary and map incompatible/invalid control generations to sanitized
`incompatible_cluster` without writes or foreign-path reads. The codec tests do
not establish zero-write incompatible startup behavior. S3 remains C08b2c.
No migration, runtime, live-authority or network guarantee is claimed here.
Independent specification and quality reviews remain parent work.

Cargo/build ownership is RELEASED on completion of this mini.
