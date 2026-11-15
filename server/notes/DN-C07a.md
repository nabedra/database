# DN-C07a loaded metrics and bounded local continuity

Implements only adopted DN-A23 mini 1 on shared main, using gpt-6.1-sol with
high reasoning, without delegation. Applied test-driven-development and unslop.
Read the distributed orchestration/testing contracts, A23 including all five
adopted refinements, relevant A05/A08/A10/A16 rules and reviewed component notes
and interfaces. Historical execution reports are background, not new evidence.
No applicable AGENTS.md was found. No R04c2 admission/payload/replay body was
inspected or used.

The owned changes are exactly:

- `server/src/cluster/heartbeat/observation.rs`: private bounded continuity state,
  closed ingestion/snapshot/live-match/failure APIs and existing observation hooks.
- `server/src/cluster/heartbeat/observation/tests.rs`: appended private component
  tests. Original tests remain a byte-identical prefix.
- `server/src/cluster/heartbeat.rs`: only opaque type reexports and their pending
  consumer comment. Writer, loader, receipt and definitive Conflict/MAX bodies
  remain byte-identical to the pre-edit copy.
- `server/src/cluster/controller.rs`: only read-only crate-private
  `OwnershipSample::monotonic() -> Instant` and `unix_ms() -> u64`. All proposal,
  evidence, guard, stamp and clock bodies remain byte-identical.
- This new delivery note.

Pre-edit snapshots retain repository-relative paths under
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-c07a-onr5ygz6/`.
They include the complete existing test text and an absence marker for this note.
Shared modified/untracked content was preserved. No declarations, manifests,
lockfiles, public API/schema, runtime, HTTP, drain selector, candidate/store module,
branch/worktree, commit/push, dependency/fetch, target cleanup, secrets/profiles,
.env or network/cloud/paid operations were performed. Cargo used the existing
`server/target`; free space before builds was about 3.7 GiB.

## Component interfaces and lifetime

All four concrete synchronous methods are crate-private:

```rust
Observations::autoscale_observe(
    &mut self, root: &Control, loaded: LoadedHeartbeat,
    sample: Result<OwnershipSample, StorageError>,
) -> Result<(), StorageError>;
Observations::autoscale_failed(
    &mut self, sample: Result<OwnershipSample, StorageError>,
) -> Result<(), StorageError>;
Observations::autoscale_snapshot(
    &mut self, root: &Control, owner: Owner,
    sample: Result<OwnershipSample, StorageError>,
) -> Result<AutoscaleSnapshot, StorageError>;
Observations::matches_autoscale(
    &mut self, snapshot: &AutoscaleSnapshot,
    sample: Result<OwnershipSample, StorageError>,
) -> Result<bool, StorageError>;
```

AutoscaleSnapshot and AutoscaleMember have private fields, no constructors for
callers, no Clone/Serde and value-free Debug. Snapshot getters borrow its complete
OwnershipObservation, paired sample, optional original low anchor and required
member slice, and return boot, exact Owner, observation generation and checked
invalidation serial. Each member borrows its exact Node, optional binding and
usable loaded heartbeat, with optional original acceptance Instant. Missing
usable facts remain absent. No backend token, mutable metrics or owned reusable
proof is exposed. The later candidate must consume the snapshot and the later
adapter must retain it through settlement; live matching and guard authorization
remain necessary.

One bounded internal state retains normalized exact root identity, terminal
classification, required member facts, one original low anchor and last paired
sample. Identity excludes ordinary root revision/change ID, scheduler diagnostics,
cooldown/backoff/failures and unrelated resolved intent progress. It includes whole
policy, admin history, config/template/timing/provisioning, exact Nodes, owner/epoch,
Held state, unresolved intents and exact retained terminal classification.
Unresolved record changes are conservatively identity changes. Fixed/external or
empty valid roots can issue coherent snapshots with absent low evidence.

The root is validated before bounded copies. Required facts are at most MAX_NODES
2048; terminal lookup is at most MAX_INTENTS 3072, with 4096 combined records and
the existing 32 MiB root bound. Terminal classification duplicates exactly
managed_capacity's retained Start Failed NotInvoked/Terminated and terminal Stop
Terminated rules. Phase-only stopped/lost/missing members remain required. All
nonterminated nodes are included without filtering ownership. The present Control
codec itself requires ownership consistent with provisioning; no mixed-ownership
schema exception was added. New classification/membership work uses BTreeSet and
binary lookup, O((N+I) log(N+I)), beyond the existing bounded C03d reconciliation
and codecs. There is no per-read growth or event history. Snapshot copies are
bounded; later runtime must admit at most one outstanding autoscale proposal.

Closed ingestion consumes a real LoadedHeartbeat, reconciles the ordered complete
root, and uses the same private sequence/validation path as raw observation.
Raw observation resets autoscale continuity. Failure, malformed/integrity roots,
old-root attempts, receipt/time paths and old deadlines close snapshots through
serial/generation or full-observation mismatch. Old-root rejection preserves the
accepted C03d root and maps. Serial exhaustion permanently closes the tracker.

The first heartbeat is baseline. Strict advancement establishes evidence; skipped
sequences are allowed. All prior required deadlines are checked before accepting
new metrics. Expiry equality closes evidence, even if the incoming sample advances.
Duplicates cannot refresh or restore evidence. Nonzero work in any of four counts,
nonacceptance and failures reset immediately, including busy then zero between
polls. Every required member must regain advancing evidence after invalidation.
Low time begins when the last required member has fresh accepting zero facts;
ordinary reads do not slide it. Drift compares nanos from the original paired
anchor with an inclusive 2000 ms tolerance; backward time and failed conversions
close proof. Persisted low_since is never inherited.

## Renewal and runtime obligations

A strictly increasing same-boot/same-owner/epoch Held extension keeps the original
low anchor provisionally and retains old freshness deadlines, but clears usable
autoscale metrics until new loaded progress. C03d still clears metrics on any
controller tuple change. The extension alone cannot resume the old metrics.

The actual later coordinator MUST successfully check the old exact Owner before
begin, record the real stamp and post-begin authorization generation, and suspend
snapshot/live-match/autoscale submission requests during the pending operation.
Only ordinary pending Unavailable with unchanged generation retains provisional
low time. Failed begin, changed/invalidated generation, failed/ambiguous/obsolete
completion, expiry or time failure must call autoscale_failed immediately, even
without a decision poll. Matching finish Ok(true), fresh same-owner check and
progressing loads before original deadlines permit resumption. No caller success
boolean or new guard authority API exists here. This discipline is a runtime
obligation, not authority inferred from lease metadata or enforced by this local
component in isolation.

Later runtime must report every missing/corrupt/failed root and heartbeat read,
continue time/deadline/load processing during renewal, sample paired time outside
locks, run bounded encoding/hash/scans on the coordinator executor, retain one
supervised submission through cancellation and run final exact-root live match
plus guard.authorize immediately before submission. These facts prove accepted
observations only; they cannot prove no work occurred between published samples.
No runtime/network/provider/RSS guarantee or aggregate C07/T07 completion is claimed.
C07b/c candidates, diagnostic writes, physical target changes and lifecycle effects
remain unimplemented. Parent assigns NEW specification review then quality review.

## Personally executed evidence

Every Cargo command used this exact environment and shared arguments:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
--manifest-path server/Cargo.toml --locked --offline --no-default-features
```

Commands below insert those arguments after `cargo test` or `cargo clippy`.
Execution order and actual results:

1. `cargo test ... --lib autoscale_loaded_progress_yields_snapshot -- --nocapture`:
   compiling stub, behavioral red, exit 101. Baseline plus advancing loaded record
   still returned Unavailable from snapshot. Build 31.57 s, test 0.01 s.
2. `cargo test ... --lib cluster::heartbeat::observation::tests -- --nocapture`:
   14 passed, exit 0, build 29.48 s, tests 0.35 s.
3. Same observation command after additions: 21 passed, exit 0, build 29.63 s,
   tests 0.42 s.
4. Same command after renewal/read cases: 23 passed, exit 0, build 29.49 s,
   tests 0.41 s.
5. Same command after negative renewal/membership cases: 26 passed, 1 failed,
   exit 101, build 29.86 s, tests 0.48 s. The failure reused an equal revision
   with divergent roots in one fixture. It correctly hit integrity rejection;
   the external root control was moved to a separate tracker.
6. `cargo test ... --lib cluster::heartbeat -- --nocapture`: 40 passed, exit 0,
   build 29.54 s, tests 0.48 s. Includes all original C03d tests and C03c
   definitive Conflict/MAX, retained effects, receipts and complete-load tests.
7. `cargo test ... --lib cluster::controller -- --nocapture`: 20 passed, exit 0,
   build 0.88 s, tests 3.72 s. Includes pending-renewal clock regression.
8. `cargo clippy ... --lib --tests -- -D warnings`: exit 0, 20.20 s.
9. `cargo test ... --lib autoscale_missing_corrupt_and_failed_load_notifications
   -- --nocapture`: exit 0, one passed, build 30.04 s, tests 0.05 s. Added actual
   Fake backend read_failure to absent/corrupt/expired-deadline controls.

One sequencing deviation occurred after step 9: the next regression Cargo command
was launched before the prior returned session ID had been confirmed exited.
The prior session was then explicitly awaited to exit 0. No Clippy overlap was
involved. This fails the requested actual-exit-before-next-launch discipline;
no claim of verified serialization is made for that pair. All other commands
were awaited before the next Cargo launch. Final additional results follow below.

Tests use actual Control/Heartbeat codecs, complete load_heartbeat and existing
crate::test_storage::Fake. Positive controls use independent baseline plus strict
advancement. Coverage includes five busy/nonacceptance dimensions, duplicates
before publication and after failures, exact expiry and one-nanosecond-before
expiry, all-member recovery, root ordering/integrity, foreign/replaced/capacity
bindings, policy/admin/terminal changes versus housekeeping, lost/startup/stopped,
empty/fixed/external fleets, restart, backward paired time, signed drift and a
one-nanosecond-above-tolerance control, exhaustion and opaque trait/redaction.
Actual OwnershipGuard begin/check/finish plus genuine proposal, PreparedOwnership,
Fake submission and OwnershipEvidence exercise 5-second renewals across a
60-second interval. Negative controls use definite failure, real lost reply with
ObservedExact, expired pending generation and obsolete stamp. Pending callers
make no autoscale eligibility request. The component tests create no detached
tasks, sleeps or network/provider effects; existing gated regressions settle their
owned effects. Later assertions have green-only provenance unless explicitly
listed as observed behavioral red.

10. `cargo test ... --lib
    autoscale_loaded_duplicate_retains_contradiction_check_after_raw_failure
    -- --nocapture`: behavioral red, exit 101, build 29.72 s, test 0.01 s.
    After raw sequence 3, read failure and an identical loaded sequence 3, a
    different loaded sequence 3 incorrectly returned Ok instead of InvalidCatalog.
    The closed path now retains every accepted loaded record for contradiction
    checks without restoring usable evidence or acceptance time on duplicates.
11. `cargo test ... --lib cluster::heartbeat -- --nocapture`: 41 passed, exit 0,
    build 30.26 s, tests 0.48 s. This includes the corrected red case and all
    16 new autoscale groups plus the affected original observation/writer groups.

Distinct full Cargo commands, each using the environment prefix above:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib autoscale_loaded_progress_yields_snapshot -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::heartbeat::observation::tests -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::heartbeat -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::controller -- --nocapture
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib autoscale_missing_corrupt_and_failed_load_notifications -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib autoscale_loaded_duplicate_retains_contradiction_check_after_raw_failure -- --nocapture
```

Owned formatting/check commands:

```sh
rustfmt --edition 2024 --config skip_children=true server/src/cluster/heartbeat/observation.rs server/src/cluster/heartbeat/observation/tests.rs server/src/cluster/heartbeat.rs server/src/cluster/controller.rs
rustfmt --check --edition 2024 --config skip_children=true server/src/cluster/heartbeat/observation.rs server/src/cluster/heartbeat/observation/tests.rs server/src/cluster/heartbeat.rs server/src/cluster/controller.rs
git diff --check -- server/src/cluster/heartbeat/observation.rs server/src/cluster/heartbeat/observation/tests.rs server/src/cluster/heartbeat.rs server/src/cluster/controller.rs server/notes/DN-C07a.md
```

Formatting was applied to the owned Rust files; the final check and scoped diff
check exited 0. Direct byte checks cover all five owned files, including untracked
content, and every snapshot/absence marker for whitespace and exactly one final
newline. Snapshot comparisons verify original test-prefix preservation and exact
narrow getter/reexport deltas. No broad suite, benchmark, default-feature or S3
build was run.

Final strict library/test Clippy repeated after the sequence correction exited 0
in 20.08 s, with no diagnostics. Every Cargo session has reached actual exit.
Rust implementation and Cargo/build ownership are RELEASED. No owned process,
future, task, gate or provider effect remains running or unsettled. This mini
awaits the parent's new specification review, followed by quality review; aggregate
C07 and later minis are not marked implemented.
