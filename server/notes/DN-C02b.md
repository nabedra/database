# DN-C02b: conditional initialization and reachable history

Implements C02b from `plans/distributed-contracts.md`, using the persistence
contract in DN-A02 and the strict codecs delivered by DN-C02a. This slice changes
only new `server/src/storage/history.rs`, new `server/tests/history.rs`, this
note, and two module/export lines in `server/src/storage.rs`. The shared main
already contained extensive uncommitted changes; those were preserved.
Decision/manifest codecs, standalone Storage methods, dependencies, commit
publication, runtime wiring and shared test support were not changed.

## Public APIs

`LoadedClusterCatalog` has private fields and read-only `catalog()` and `etag()`
accessors. It pairs a validated v3 root with its nonempty opaque backend token.
A non-null decision head must equal the root commit UUID. A null head may retain
a nonzero revision and legacy commit UUID after migration.

`Storage::load_cluster_catalog()` reads at most MAX_CATALOG_BYTES using Storage's
complete bounded reader, then strictly decodes v3 and checks its head relation
and token. It never invokes migration. The existing standalone `load_catalog()`
continues rejecting v3.

`Storage::initialize_cluster_catalog()` reads once, attaches valid v3 without a
write, creates missing v3 from Catalog::empty with Create, or migrates validated
v1/v2 using Match with the exact loaded token. Migration preserves revision,
commit UUID, table UUIDs, schemas and immutable snapshot/partition references and
manifests via ClusterCatalog::from_catalog. The old standalone writer MUST be
stopped before migration. This is an operator precondition; the helper does not
attempt to establish remote process absence.

A small typed version probe selects the decoder. The selected strict codec
receives the original bytes, so neither top-level nor nested duplicate-key
evidence is erased by generic Value conversion. Malformed, unsupported,
truncated, unreadable or empty-token roots are never reset. There is at most one
PUT attempt per helper call. Conflict or Ambiguous causes one fresh read: valid
v3 is returned, a still-valid legacy/missing root retains the corresponding
conditional/ambiguous error, and corrupt or unreadable reconciliation remains an
error. A success response lacking a token also requires reconciliation as an
ambiguous write. There is no blind second PUT or repeated migration.

`HistoryBudget::new(max_records, max_bytes, tokio::time::Instant)` requires
positive count/byte limits and a future absolute monotonic deadline. Zero limits
return LimitExceeded; an expired deadline returns Unavailable. Fields are private
and lookup rechecks the deadline. Runtime defaults are deliberately not chosen.

`Storage::lookup_decision(&LoadedClusterCatalog, Uuid, &[u8], HistoryBudget)`
returns `HistoryLookup::{Found(Decision), IdentityConflict, Absent}`. The UUID is
normalized by its Uuid type; request bytes are compared exactly. Found retains
the complete stored terminal response status/body. IdentityConflict carries no
body and is distinct from storage CAS Conflict. Authorize before lookup. The
caller must use a captured root from the same durable namespace.

Only typed decision keys reachable from that root are read. Each record passes
strict bounded Decision decoding, expected key UUID, expected revision and
checked revision decrease. The first revision equals the root revision; each
older revision equals the newer record's base revision. This catches gaps and
cycles using constant auxiliary space. Null may terminate above revision zero
because migration starts with an empty history. A matching valid reachable
prefix suffices for Found or IdentityConflict; older objects need not be read.
Only a valid traversal reaching null returns Absent. Missing referenced objects
are InvalidCatalog; corrupt, partial or unavailable reads remain errors.

Counters use checked arithmetic. Each read uses
`min(MAX_DECISION_BYTES, remaining cumulative encoded bytes)` and inherits
Storage's at-most-one-byte excess probe. Deadline enforcement wraps both backend
GET and body consumption with timeout_at and checks time around decoding and
before returning. A custom backend must cooperate with asynchronous polling;
Tokio cannot preempt a backend that blocks its executor thread synchronously.
Count/byte exhaustion is LimitExceeded and deadline exhaustion is Unavailable,
never Absent. Exact limits permit a valid null terminus. Lookup retains one
bounded record at a time, not the chain or every response. Cost grows linearly
with retained history. There is no TTL, garbage collection, cache, index or retry
framework.

## Evidence and commands

Applied the test-driven-development skill. Seven grouped behavior tests were
written before implementation. The initial history command exited 101 with
missing HistoryBudget/HistoryLookup and missing Storage-method diagnostics.
This was a compilation failure for absent APIs, not an assertion failure; no
tests ran. After implementation the same command passed all seven tests.

Coverage includes missing initialization, exact v1 snapshot/v2 partitioned
identity migration, existing headless/headful v3 attachment without overwrite,
applied-but-response-lost creation/migration, invalid roots and failed
reconciliation, terminal replay after later commits, exact body mismatch,
unreachable orphan exclusion, null and migration-boundary absence, wrong IDs,
revision gaps, cycles, missing/corrupt/partial records, exact and insufficient
byte/count budgets, and pending GET/body deadlines.

The owned concurrent fixture captures both initial GET results before releasing
either initializer to PUT, using a Tokio barrier. Both create and migration
produce two conditional attempts and exactly one applied backend generation.
The lost-response tests verify a persisted v3 root and generation increment,
so the evidence proves that the write applied before reconciliation. No sleeps
establish overlap. These are in-process backend-CAS tests; real multiprocess TCP
acceptance remains later work.

All Cargo calls used existing server/target and this exact prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Commands, sequentially, from repository root:

```sh
# Initial missing-API red, then first green: 7 passed.
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test history

# Final strengthened history tests and storage regression, run once together.
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test history --test storage

CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test history --test storage -- -D warnings

rustfmt --edition 2024 --check --config skip_children=true server/src/storage.rs server/src/storage/history.rs server/tests/history.rs
```

Final result: history 7 passed, storage 8 passed, strict targeted Clippy passed,
and owned rustfmt passed. No broad suite, AWS/default-feature build, dependencies
installed, target cleanup/new target directories, cloud calls, .env reads,
delegates, branches/worktrees, commits or pushes. Cargo ownership is released on
delivery. Ready for independent specification and quality review.

## Next integration responsibilities

C02c must atomically publish table changes and a prepared decision head in one
catalog CAS, verify candidate/current ID/parent/base-revision relationships, and
record no-op successes and deterministic mutation errors. The runtime must
choose explicit per-attempt budgets, authorize before lookup, preserve original
request/response bytes, and reconcile conflict/ambiguity before recomputation.
An incomplete lookup must never authorize reexecution. Captured absence alone
does not prove that an earlier pending CAS failed. Cluster startup must call the
one-way initializer under the stopped-standalone precondition. Replay HTTP
integration and actual separate-process TCP acceptance remain outside C02b.

## Quality correction: opaque token diagnostics

Specification review passed. Quality review found derived Debug exposed the
private backend token. The coordinator added a sentinel assertion to the
existing reachable-history scenario. With the same Cargo resource flags above,
`cargo test --manifest-path server/Cargo.toml --locked --offline
--no-default-features --test history
reachable_terminal_replay_identity_conflict_orphan_and_absence` failed at that
assertion before correction. A manual Debug implementation now includes only
revision and commit UUID, omitting the token and catalog contents.

The same focused scenario then passed. Targeted strict Clippy for the library
and history test passed in 6.04 seconds; owned rustfmt and direct whitespace
checks passed. No persistence or lookup logic changed, so the other passing
history/storage scenarios were not repeated. Fresh independent quality re-check
passed without remaining actionable findings. The C02b slice is complete.
Cargo ownership is released.
