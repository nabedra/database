# DN-C02a: bounded cluster metadata formats

Implemented only the pure C02a data formats from `plans/distributed-contracts.md`.
No storage reads/writes, history lookup, execution, publication, or migration were
added. No dependencies or manifest validators changed. `storage.rs` only gains
the private module declaration and public codec exports. Existing Catalog and
Storage.load_catalog behavior remains intact. No applicable AGENTS.md was found
in the workspace ancestry or owned file directories.

## APIs and validation

`storage::{ClusterCatalog, Decision, MAX_DECISION_BYTES}` are public.
Both records expose `decode(&[u8])`, `encode()`, and `validate()` returning
sanitized existing StorageError categories. Their fields expose IDs, parent/head,
and revisions for later relationship checks. Constructed candidates are checked
again at encoding. Use these bounded entry points for persisted bytes; derived
Serde traits are not validation entry points. Decision Debug includes only its
ID and revision, never bodies. Authentication headers are not codec inputs.

ClusterCatalog has exactly format_version=3, revision, commit_id, tables, and
required nullable decision_head. `from_catalog(&Catalog)` validates a v1/v2
catalog and prepares a v3 value with the same revision, commit UUID, tables,
IDs, and manifests, and a null head. This conversion performs no I/O.
Decoding preserves raw table JSON in a small version-2 view and delegates to
Catalog::decode for nested object shapes, duplicate names and all existing
schema/key/path/partition validations. Encoding reuses Catalog::validate.
No shared-helper visibility changes were necessary.

Decision v1 has exactly version, id, required nullable parent, base_revision,
revision, request_id, request_body, response_status, and response_body. It rejects
self-parenting and requires checked base_revision + 1 == revision. Request bytes
pass existing parse_request and must correlate to the normalized UUID and a
mutation. Original body strings are retained without normalization, including
whitespace, field order, numeric spelling, and UUID spelling. They contain only
the public JSON body, not transport authentication metadata.

Responses must be object envelopes with exactly correlated request_id/data for
status 200, or request_id/error for deterministic validation_error, not_found,
conflict, or limit_exceeded with the existing status code and not_applied.
Error objects reuse ProtocolError validation, with an explicit object guard.
Unknown fields, duplicate envelope/error fields, missing required fields, array
struct encodings, and non-integer metadata numbers reject. Operation-specific
successful data validation remains the existing encoder/coordinator's job.
Reads, malformed/unsupported requests, authentication errors, overload, storage,
internal failures, and unknown outcomes cannot become terminal decisions.

## Limits and scope

The catalog encoded/decoded cap is 1 MiB. Each original body is at most 8 MiB.
Decision encoded/decoded cap is `4 * MAX_BODY_BYTES + 64 * 1024`, or 33,619,968
bytes. Input caps are checked before deserialization. Encoding uses a bounded
writer throughout, including the temporary legacy catalog view. The decision
cap is independent of the 16 MiB node CONTROL cap. A focused escaping fixture
uses one 8 MiB request plus a 4 KiB response, both padded with valid JSON tab
whitespace, to cross 16 MiB without allocating two maximally escaped bodies.

These codecs do not make a decision authoritative, establish replay, fence a
commit, deduplicate execution, or prove multiprocess publication. C02b must load
the complete reachable history and perform conditional migration. C02c must
atomically publish the catalog root plus decision reference and check the
ID/parent/base-revision relationships. An orphan object proves no publication.

## Verification

All Cargo calls were sequential and used the existing server/target with this
exact environment prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

The first test was written before production code. Initial command:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test decision
```

It exited 101 with E0432 for missing ClusterCatalog. No tests executed. After
adding the grouped tests and before implementation, the same command exited 101
with two E0432 diagnostics for missing ClusterCatalog, Decision, and
MAX_DECISION_BYTES. These were compilation failures for absent APIs, not runtime
assertion failures.

After implementation:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test decision --test storage
```

Initial green run: decision 5 passed, storage 8 passed, zero failures. A subsequent
test-only strengthening run produced decision 4 passed / 1 failed because 4,000
valid catalog entries still fit the cap. Increasing the fixture to 6,000 valid
entries made the bound assertion meaningful. Subsequent runs passed all 5
decision tests and all 8 unchanged storage tests. Final reduced-buffer escaping
fixture also passed these same 13 tests.

```sh
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test decision --test storage -- -D warnings
rustfmt --edition 2024 --check --config skip_children=true server/src/storage.rs server/src/storage/decision.rs server/tests/decision.rs
```

Strict targeted Clippy and owned formatting passed. No full suite, default AWS
build, cloud calls, .env access, branches, worktrees, commits, pushes, or delegates.
Cargo ownership is released on delivery.

## Specification correction: terminal enum wire types

The specification review found that derived Serde enum decoding accepted
object-valued unit variants for error code/outcome. The coordinator added both
forms to the existing malformed-record scenario, then ran:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test decision rejects_nonterminal_uncorrelated_and_malformed_records
```

It failed with `object-valued code accepted` before the correction. The codec
now requires both JSON fields to be strings before the existing ProtocolError
decoder applies its enum, duplicate-field and envelope validation. It still
decodes the original raw error object, so the preliminary shape inspection
cannot erase duplicate-field evidence. The large escaped-body case now compares
both decoded body strings with their exact originals, without printing huge
body contents on a failure.

Using the same resource flags as above, `cargo test --manifest-path
server/Cargo.toml --locked --offline --no-default-features --test decision`
passed all five grouped tests in 1.78 seconds. Targeted strict Clippy for the
library and decision test passed. Owned rustfmt and direct whitespace checks
passed. Existing storage tests were not repeated because storage behavior did
not change. Independent specification re-check and code-quality review both
passed without remaining actionable findings. Reviewers reused this evidence
without redundant builds or tests. This completes the codec slice only.
