# DN-C02c1: prepared metadata publication

Implements only C02c1 under the persistence contract in
`plans/distributed-contracts.md`, DN-A02, and reviewed DN-C02a/b. Existing legacy
commit behavior is unchanged. This slice does not execute operations, validate
rows, encode public responses, or upload table objects.

Owned changes are new `src/storage/publication.rs`, `tests/publication.rs`, this
note, the narrow storage module/export additions, and one crate-visible
`HistoryBudget::deadline()` accessor. No history lookup or codec logic changed.
The extensive pre-existing shared work was preserved. No applicable AGENTS.md
was found. Applied the test-driven-development and unslop skills. No delegation,
branches/worktrees, commits/pushes, dependencies, cloud, or .env access.

## API and caller sequence

`PreparedPublication::prepare(LoadedClusterCatalog, BTreeMap<String,
CatalogEntry>, Decision)` is pure and returns a candidate with private fields and
encoded bytes. It is neither Clone nor externally mutable. Debug prints only
commit UUID and revision, never table contents, bodies, or opaque tokens.
`commit_id()` is the only metadata accessor. The caller supplies the complete
proposed table map. Preparation compares it against the captured base and allows
zero or one changed table, preserving all unrelated entries. Terminal errors
cannot change tables. Existing table descriptors cannot change, and partitioned
data cannot regress to legacy layout. Catalog/schema/manifest validation is
reused. Object ownership and retained physical facts are the caller's duty.

Preparation checks a fresh decision UUID relative to the base commit, exact
parent/head relation, base revision and checked successor revision. It invokes
the Decision codec for the exact original request and final response, then
encodes a bounded v3 root with the decision UUID as commit and head. Both
encodings finish before preparation returns. It does not imply table-object
verification or upload has happened.

C02c2 must perform this sequence in one durable namespace:

1. Authorize and complete replay lookup against a captured root.
2. Compute and validate the whole mutation, rows, schema and final response.
3. Prepare immutable metadata, including all encoding and size checks.
4. Verify ownership, retained facts and all newly referenced immutable table
   objects, and upload those validated objects before root publication.
5. Consume the candidate with `publish(&Storage, HistoryBudget)`.

This API accepts prepared metadata; it cannot establish operation-specific
response correctness or that arbitrary table inputs represent an executed
mutation. C02c2 owns those checks, contention retries/recomputation, admission,
and public response mapping. It must track prior unresolved logical attempts.

## Phases and outcomes

Publication first searches the captured reachable history. A stored matching
UUID/body returns the original terminal Decision and exact response. A different
body returns IdentityConflict. Incomplete history returns an attempt error and
never permits a decision upload or catalog PUT.

After complete absence, publication makes one Create-only decision upload using
the typed decision key. Conflict, Ambiguous or an empty success token triggers
one bounded read and exact encoded-byte comparison. Even a valid equivalent
JSON representation with different bytes cannot be reused. Failure or mismatch
stops before root submission. A confirmed orphan alone proves no publication.
There is no hidden upload retry.

Publication checks the absolute deadline and makes at most ONE catalog Match PUT
using the exact captured token. A nonempty success token confirms Terminal.
Conflict triggers one fresh valid-root read and reachable-history lookup. Found
returns Terminal, a body mismatch returns IdentityConflict, and complete absence
returns NeedsRecompute. That result requires a newly computed candidate against
a new base; this candidate is consumed and never resubmitted on a newer token.
Incomplete reconciliation returns Unknown.

An empty root success token, timeout, or other root failure conservatively
triggers the same reconciliation if time remains. A matching reachable decision
anywhere proves Terminal even after later commits. IdentityConflict remains
authoritative. Absent, missing, invalid, unreadable or incomplete recovery is
Unknown, because a pending PUT might still win. The implementation conservatively
uses Unknown even for definitely unsubmitted backend root errors. It never maps
backend Conflict to an HTTP not_applied result.

`Result<PublicationOutcome, StorageError>` distinguishes Terminal(Decision),
IdentityConflict, NeedsRecompute and Unknown from pre-root attempt errors. Err
means this invocation did not submit its root. It does NOT promise no other
attempt of the logical request committed. Likewise NeedsRecompute does not
settle an earlier unresolved attempt. No object deletion, cleanup or rollback
occurs on uncertainty. No local/global mutation mutex or parent-process lease
is introduced. Once root submission may have started, dropping/cancelling this
future cannot prove rollback. The caller must retain admitted work and reconcile
across disconnects, following the existing executor's retention contract.

## Bounds

The caller explicitly supplies HistoryBudget with count N, cumulative encoded
bytes M and one absolute Tokio monotonic deadline D for the whole invocation.
Each of at most two history scans independently receives N/M, but both share D.
Worst case is 2N history reads consuming at most 2M encoded bytes, one fresh root
read capped at MAX_CATALOG_BYTES, and one immutable confirmation read capped at
MAX_DECISION_BYTES. Each bounded read may probe one excess byte. There are at
most one decision Create and one root Match, each bounded by the codec's cap.
The captured root was loaded by the caller before invocation; it is not another
read inside publish. History lookup retains one record at a time.

All GET/body awaits and PUT awaits share D. Pre-root deadline expiry is an
Unavailable attempt error. A timeout of a possibly submitted root is Unknown;
there is no second root PUT. No new timeout window is created for recovery.
Synchronous validation/decoding checks cannot be preempted by Tokio, and the
backend must cooperate with async polling, as already required by C02b. There
are no runtime budget defaults or background tasks.

## Test evidence

The local conditional backend atomically checks Create/Match and replaces bytes
under its state lock. Tokio barriers gate competing root submissions after both
candidates were prepared. Semaphore gates stop a root before its condition or
stop the response AFTER its bytes actually applied. No sleep establishes overlap.
These tests are in-process evidence only; actual TCP/multiprocess acceptance
remains E04.

Ten grouped tests cover atomic tables/head advancement and exact replay;
decision-only no-op/error history; invalid relationships, revision overflow,
invalid schema and error/table changes; request and encoded-root size limits;
immutable corruption and valid-but-different bytes; lost immutable response
confirmed without resend; same-base races for different IDs, same UUID/body and
same UUID/different bodies; applied A with lost reply, then B, then recovery
through B to stored A; orphan exclusion, missing/failed/budget-limited history;
valid absence after ambiguity; empty success token; pre-root versus pending-root
deadline classification; same-ID recovery fencing a gated original CAS; and
failed reconciliation after an applied root. Assertions verify persisted root
IDs/revisions and count root attempts, not just simulated transport faults.

All Cargo invocations were sequential, used existing `server/target`, and had
this exact prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Observed commands from repository root:

```sh
# Initial red: exit 101, E0425/E0433 for absent PreparedPublication and
# PublicationOutcome; no tests executed.
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test publication

# Same command after implementation: 6 passed / 1 failed. The retry fixture
# changed the request UUID without correlating its response UUID. Fixed the
# fixture with a valid, different proposed response to check stored-response replay.
# Same command with two extra focused cases: 9 passed.

# Final strengthened publication plus relevant codec/history regression once:
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test publication --test history --test decision
# 10 publication + 7 history + 5 decision passed, zero failures.

CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test publication -- -D warnings

rustfmt --edition 2024 --check --config skip_children=true server/src/storage.rs server/src/storage/history.rs server/src/storage/publication.rs server/tests/publication.rs
```

Strict targeted Clippy, owned rustfmt and direct whitespace checks passed.
No full suite, AWS build, new target directory or target cleanup. Ready for parent
orchestration of independent specification review followed by quality review.
Cargo ownership is released on delivery.

## Independent review disposition

Specification review and subsequent code-quality review both passed without
actionable findings. They inspected the implementation, contracts, causal test
fixtures and recorded verification above; neither repeated passing suites.
The reviewed evidence covers the in-process publication primitive. Engine
integration, object uploads and actual TCP/process acceptance remain pending.
