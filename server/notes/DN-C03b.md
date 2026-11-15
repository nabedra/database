# DN-C03b consumed membership publication

Implements only adopted DN-A16 assignment 2. Applied test-driven-development
and unslop. Inspected distributed-contracts.md, the C03/testing rubric,
A16's interface table and assignment, A05/A07/A08/A10/A11 semantics, reviewed
C03a/C04b1/C04b2a/b/c and relevant member/root codec notes, DN-test-lints.md,
and the actual store, controller observation, membership candidate and Fake
interfaces. No applicable AGENTS.md was found in the ancestors or owned tree.
Earlier notes' passing results and the parent's review dispositions are reported
evidence; the commands below were personally executed for this assignment.

Owned changes are new `src/cluster/store/membership.rs`, its private
`membership/tests.rs`, this note, the private membership declaration/reexport
and shared submit helper extraction in `src/cluster/store.rs`, and sibling-test
visibility for the existing root fixture in `store/tests.rs`. Existing ownership
assertions are unchanged. Shared uncommitted/untracked work is preserved.
No controller, membership-candidate, codec, admin, attachment, authentication,
heartbeat, runtime, plan, manifest, dependency or lockfile edits were made.
R03a3a sequence code is not imported or used as a dependency.

The exact crate-private API is:

```rust
PreparedMembership::prepare(
    base: LoadedControl,
    candidate: MembershipCandidate,
) -> Result<Self, StorageError>
PreparedMembership::submit<B: ObjectStore>(
    self,
    storage: &Storage<B>,
    deadline: tokio::time::Instant,
) -> Result<OwnershipOutcome, StorageError>
```

PreparedMembership is opaque, non-Clone, must-use and consumed by submission.
Its fields retain the loaded base with captured token, validated candidate and
bounded encoded bytes. Debug prints only `PreparedMembership`. There is no
mutation, token accessor or arbitrary byte/token constructor. Preparation
consumes C03a's into_parts, compares its complete OwnershipObservation with a
new validated observation of the loaded base, rechecks held state and exact
owner/epoch, and requires the entire candidate controller tuple to equal the
base tuple. Mismatches return existing Conflict. Bounded candidate encoding
uses existing InvalidCatalog/LimitExceeded mappings. The opaque C03a constructor
continues to own phase, proposal, revision and admission restrictions.

Private `submit_control(base, candidate, bytes, storage, deadline)` contains the
existing PreparedOwnership::submit body, with owned arguments replacing field
accesses. Both prepared types consume it. Ownership preparation still calls
ownership_candidate and encodes exactly as before. Inspection found no changed
classification, deadline or counter branch in the extracted body.

The helper checks pre-write expiry as definite Unavailable and makes at most
one Match using the captured token. Only a timely nonempty returned token gives
Acknowledged. Conflict remains Conflict even if readback fails or equals the
candidate. Ambiguous, empty success or timeout permits at most one bounded
load_control under the original deadline. Only complete candidate equality and
proposal UUID equality gives ObservedExact. Later/different roots, same UUID
with different contents and missing/corrupt/failed reads remain Unknown after
possible application. Other definite adapter errors preserve their categories.
No retry, unconditional write, new timeout, history scan or inference of
nonapplication from a later root was added. Outcome names grant no authority.
Item-scoped dead-code allowances on the new struct/impl and unused-import
allowance on its reexport await the membership runtime caller. No module-wide
lint suppression was added.

Every Cargo command used existing server/target and this exact environment:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Personally executed commands after that environment prefix:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::store::membership::tests -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::store::membership::tests::competing_registrations_and_exact_binding_recovery
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::store:: -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::membership::tests::consumed_parts_diagnostics_and_retained_stop_evidence
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

The first attempt had a compile-only E0433 fixture-module path error; no
behavioral red is claimed for it. After correcting that path, five groups
compiled against rejecting prepare/submit placeholders: four failed at valid
preparation with unexpected Conflict, one mismatch group passed. Exit 101,
34.16s compilation, 0.03s tests. After implementing preparation alone, the
single competing-registration group failed at submission's rejecting placeholder,
exit 101, 35.02s compilation, 0.02s tests. This is direct submission red.
After extracting the existing helper and connecting submit, all seven membership
groups passed, 177 filtered, 26.34s compilation, 0.08s tests. Delayed-provider,
pre-expiry and later strengthened assertions have green-only provenance;
there is no separate red claim for every assertion or reused helper branch.

After strengthening fixtures to retain the actual captured condition, use a
signed replacement proof, precompute encodings before gated work, and record
pending submission conditions, the store filter passed all 28 tests, 156
filtered, 32.59s compilation, 0.89s tests. This filter includes 14 existing
ownership/guard tests, seven new membership tests and seven existing admin tests.
The narrow C03a consumed-parts regression passed one test, 183 filtered,
1.76s build, 0.02s tests. Strict library/test Clippy passed without diagnostics,
exit 0, 29.70s. No full suite or default/AWS build was run.

The new groups use the single cfg(test) crate::test_storage Fake, real bounded
Control codecs and actual verify_enrollment. Canonical enrollment text is
assembled independently and signed with a synthetic ring Ed25519 seed. No
unchecked proof constructor or real secret is used. The baseline adapts the
existing generation-2 root fixture to a valid external held-owner root, then
obtains registered/readiness/replacement candidates through C03a. These tests
do not repeat C03a's managed eligibility or codec matrices.

Tests cover competing same-token registration with one winner; reload of exact
claims as Existing and changed-session conflict; lost committed reply with exact
current readback; an unrelated later root yielding Unknown while exact binding
retry recovers the unchanged session; before-Match registration fencing by
takeover or a competing registered/draining root; readiness fencing by drain or
replacement; failed/missing/corrupt/empty-token reconciliation and equal UUID
with unequal valid contents; pre-write expiry; and complete-base/owner mismatch
rejection before storage. Whole-root comparisons prove stale publication does
not restore the previous phase or owner. The competing drain/replacement writes
are encoded valid roots submitted directly through the Fake's Match boundary;
they are synthetic interleavings, not C06/runtime transition evidence.

Before/after-application and read gates establish causal order. Gate waits and
task settlement have five-second watchdogs; every new gate is released and
owned tasks settle or abort-and-join before outcome assertions. No mutex spans
await. A test-local delayed-provider handle is retained and joined, never
intentionally detached. Its ambiguous return precedes actual Fake application:
one reconciliation observes the old root, yet releasing the original later
commits unless a separate successful captured-token Match has fenced it. The
submission-condition log proves one original PUT invocation; Fake's PUT count
increments only on eventual condition evaluation. Ordinary ambiguous cases
assert one PUT and one reconciliation GET, in addition to the initial base GET.
These are in-process Fake/codec tests, not TCP/provider/authority evidence.
Readiness initialized/challenge fields remain explicit caller assertions, not
claims of authenticated probes or live authorization.

Later runtime must run finite encoding/observation calculations on the bounded
coordinator CPU/I/O executor outside short locks, capture guard generation at
admission, and finally authorize the exact fresh complete observation with the
expected Owner and fresh time before polling submission or accepting an exact
retry. Membership acknowledgment does not install ownership certainty. Pending
writes require retained supervision across cancellation, and enrollment
acknowledgment requires fresh recovery and final authority checks. The bounded
projected-status admission gate, real authenticated initialized probes,
heartbeat/expiry generation checks and actual routes remain later work.
No aggregate membership or runtime completion is claimed.

Final formatting and whitespace/newline checks are recorded below. An initial
rustfmt invocation misspelled `--edition 2024` as `--edition2024` and was rejected;
the corrected owned formatting command succeeded. No target deletion, fetch,
network/cloud call, .env/real-secret/profile read, delegation, branch/worktree,
commit or push occurred. Observed disk headroom remains approximately 2.3 GiB.

Final owned checks passed, exit 0:

```sh
rustfmt --check --edition 2024 --config skip_children=true server/src/cluster/store.rs server/src/cluster/store/tests.rs server/src/cluster/store/membership.rs server/src/cluster/store/membership/tests.rs
git diff --check -- server/src/cluster/store.rs server/src/cluster/store/tests.rs server/src/cluster/store/membership.rs server/src/cluster/store/membership/tests.rs server/notes/DN-C03b.md
```

A direct byte check of those five files, including untracked files, confirmed
final LF and no trailing spaces/tabs. Git diff alone cannot inspect untracked
contents. No Rust behavior changed after the passing store run.

Cargo/build and Rust implementation ownership is explicitly RELEASED to the
parent. Independent specification review, followed by separate quality review,
remains parent-owned. No essential interface gap was encountered.
