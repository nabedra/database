# DN-C03d local heartbeat observations

Implements adopted DN-A16 mini4 and A21's narrow DrainHeartbeat seam only.
Shared main, modified/untracked files, existing target and Cargo/lock were
preserved. No delegation, branch, worktree, commit, push, dependency/fetch,
network/cloud/paid call, secret/profile access or target cleanup was used.
No unfinished R04 implementation was inspected or used. Read the distributed
contracts and rubric orchestration/testing/C03/F09/F14/C06 requirements, A16's
common interface and mini4 corrections, A21 receipt/C06b composition, A05
heartbeat/takeover refinements, A10 ordering, reviewed C03a/b/c including the
parent Conflict/MAX correction, C04b2 guard and CLOSED DN-test-lints. Inspected
actual membership ExpiryInput/loss_candidate, Control/member codecs,
OwnershipObservation, heartbeat loader/binding and opaque loaded getters.
No applicable repository/ancestor AGENTS.md was found. Applied TDD and unslop.

## Owned files and snapshot

- New `server/src/cluster/heartbeat/observation.rs` and private
  `observation/tests.rs`.
- `server/src/cluster/heartbeat.rs` adds only the private child declaration,
  crate-private reexports with a pending-consumer import allowance, and removes
  the now unnecessary LoadedHeartbeat struct/impl dead-code allowances.
- This note.

The pre-edit parent file is preserved outside the repository at
`/tmp/meshdb-c03d.LG6Cro/heartbeat.rs.before`. A direct comparison confirms no
writer, loader, receipt or corrected Conflict/MAX body changes.

## Concrete APIs and evidence

All methods are synchronous, crate-private and use explicit tokio Instants.
`Observations::new(controller_boot: Uuid, now: Instant) -> Self` creates one
boot-owned tracker. Its mutable methods are:

```rust
reconcile_members(&mut self, root: &Control, now: Instant) -> Result<(), StorageError>
observe(&mut self, binding: &HeartbeatBinding, heartbeat: &Heartbeat, now: Instant) -> Result<(), StorageError>
read_failed(&mut self, binding: &HeartbeatBinding, now: Instant) -> Result<(), StorageError>
fresh(&mut self, binding: &HeartbeatBinding, now: Instant, expiry: Duration) -> Result<bool, StorageError>
expiry_input(&mut self, binding: &HeartbeatBinding, now: Instant, expiry: Duration) -> Result<Option<ExpiryInput>, StorageError>
matches_expiry(&mut self, binding: &HeartbeatBinding, captured: &ExpiryInput, now: Instant, expiry: Duration) -> Result<bool, StorageError>
drain_heartbeat(&mut self, root: &Control, binding: &HeartbeatBinding, loaded: &LoadedHeartbeat, now: Instant, expiry: Duration) -> Result<DrainHeartbeat, StorageError>
matches_drain(&mut self, receipt: &DrainHeartbeat, now: Instant) -> Result<bool, StorageError>
```

State is private and bounded by validated Control membership, with one vector
entry per eligible registered starting/ready/draining member, a complete
OwnershipObservation, controller tuple, checked generation and monotonic time
continuity. Reconciliation transiently retains at most the old and new bounded
vectors. There is no read-key map, event abstraction, RNG, wall clock or I/O.
Debug prints type names only. Item/impl dead-code allowances name the actual
pending coordinator and C06b consumers; there is no module lint suppression.

Root validation and complete encoding/hash occur before ordering. Older roots
return Conflict before time/map mutation. Equal-revision complete disagreement
returns InvalidCatalog, clears freshness and invalidates captures while retaining
the accepted root, binding and sequence high-water. Legitimate newer roots prune
obsolete bindings; exact cluster/node/incarnation/session/verifier/capacities
are required for entry reuse. Full Node/root equality protects resource, phase,
drain and other metadata associations. Root progress with unchanged controller
preserves original grace and advance anchors. Controller changes clear metrics,
retain known sequence and start full grace. A new tracker cannot inherit the
old process's local freshness or generation.

The first positive sequence for a binding establishes a baseline. A strictly
higher sequence establishes fresh metrics at local acceptance time and full
grace. This conservative baseline rule also applies after process restart.
A takeover retaining a known sequence can recognize a genuinely higher read.
Repeated sequences never change metrics/advance time or restore invalidated
freshness. Lower, invalid, capacity-mismatched or contradictory equal-sequence
records reject and close evidence. Obsolete bindings conflict without replacing
current entries. Read failure clears current metrics and advances generation,
retaining sequence and original anchors; repeated failures cannot postpone loss.

All time-checking methods mutate failure state. A regressed Instant closes
freshness, invalidates captures and requires a later valid sample to start full
new grace. Sequence high-water survives. A normal sample alone cannot restore
metrics. Checked Instant addition failure or zero duration fails closed with
LimitExceeded; monotonic regression uses Unavailable. Checked generation
exhaustion permanently closes this tracker, with no reset/wrap. Freshness uses
strict `now < last_advance + expiry`; loss needs both grace and advance deadlines.
A missing-heartbeat entry uses tracking grace without a fictitious sequence.
Draining entries never produce ExpiryInput.

ExpiryInput retains the exact Node, complete observation, boot/generation,
sequence, anchors and sample. `matches_expiry` recomputes current expiry and
compares every association and anchor before candidate construction/submission.
A higher heartbeat invalidates captures even with the same durable root token.
The runtime must still compare under its live owner/generation and perform
final A10 authorization against the exact complete observation before polling
the captured-token submission. The comparison grants no authority.

DrainHeartbeat has private fields, no raw/Serde constructor, no Clone and no
token accessor. It is issued only from an actual opaque LoadedHeartbeat with
an exact current binding and an ordered validated root, held controller matching
this boot, accepted progressing sequence, valid metrics and matching current
drain. Expiry must exactly equal the root's configured expiry, so a caller
cannot enlarge the proof window. A first old high baseline grants no receipt.
Read failures, lower/contradictory records and takeover invalidate freshness.
Read-only getters expose `observation`, `controller_boot`, `generation`,
`binding`, `member`, `heartbeat` and `sampled_at`; matches_drain rechecks live
root/generation/metrics and configured freshness before later composition.

C06b must still authenticate the exact reply/request/round, check the barrier,
positive acknowledged sequence, accepting=false and all four zero occupancies,
and recheck live authority. The receipt can retain nonzero/accepting metrics for
that consumer to reject. It grants no idle, routing, stop, termination, credit
or replacement authority. Stale draining members remain draining for continued
authenticated reconciliation. Routing additionally requires durable Ready and
all authenticated initialization checks. No runtime, HTTP, periodic polling,
controller loop, C06 proof or fresh-network guarantee is implemented.

## Verification and provenance

Every Cargo invocation used the existing server target and the exact prefix
below, with each actual process exit awaited before the next invocation:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Commands use `--manifest-path server/Cargo.toml --locked --offline
--no-default-features`. No default/S3 or full-suite build was run.

Initial observation test against closed stubs first failed fixture validation
because its external resource identity had the wrong format. Correcting the
synthetic fixture to `external:<node_id>` reached a meaningful behavioral red:
`repeats_expire_at_original_advance` failed the fresh assertion, exit 101,
27.87s build. The first implementation passed five focused tests, exit 0,
31.16s build, 0.01s tests. Expanded explicit-time/Fake tests then passed eleven,
exit 0, 32.43s build, 0.40s tests. These additional cases have green-only
execution evidence, not individual observed behavioral reds. Added the live
drain comparison and captured-sample getter, then the heartbeat filter passed
23 groups including twelve existing writer/loader groups, exit 0, 27.08s build,
0.34s tests. The affected membership filter passed eight groups, exit 0,
0.93s build and 3.07s tests. Strict library/test Clippy passed, exit 0, 20.34s.

The tests use actual generation-2 complete root encoding/decoding and the existing
crate::test_storage Fake with complete load_heartbeat, not raw receipt creation.
They cover repeat expiry at the original interval, independent binding/capacity
controls, immediate failed-read denial, advancing recovery, failure repetition,
regression through both query APIs, overflow/exhaustion, takeover/restart and
baseline/progress distinction, root ordering/integrity, replacement/pruning and
2048-entry bounds. Actual loss_candidate preserves the whole root except loss
and proposal identity, including a retained managed start, credits, controller,
admin chain and scheduler. Drain tests cover stale/failed/lower evidence, wrong
boot/root/binding/drain, configured expiry mismatch, takeover, redaction and
negative Clone/Serialize traits. No signature/codec/storage matrix is duplicated.
Async fixture I/O is directly awaited; no detached tasks, timers or sleeps exist.

Exact command forms executed so far:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::heartbeat::observation
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::heartbeat
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::membership
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

Later runtime calls finite encoding/hashing/cloning on its bounded coordinator
executor outside locks, bounds each complete read and uses acceptance time,
reports failed/missing/corrupt reads before using observations, supervises async
owners, and rechecks live generations and A10 authority at submission. These
caller obligations remain unfinished. Parent performs a NEW independent spec
review followed by quality review; this note does not substitute for either.

## Final anchor correction and release

Self-review found that resetting last_advance on takeover or time-continuity
recovery would mislabel a grace reset as an actual heartbeat advance. Added
`takeover_grace_does_not_manufacture_sequence_advancement_time` before correcting
production. This exact focused command failed behaviorally at the last_advance
assertion, exit 101, 30.11s build and 0.01s tests:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::heartbeat::observation::tests::takeover_grace_does_not_manufacture_sequence_advancement_time -- --exact
```

The correction resets only grace in both paths, preserving the actual last
accepted sequence-advance time. Both intervals must still elapse; the new grace
provides the required full interval. Repeated normal samples never restore
freshness. With the same required environment prefix, the final observation
filter passed all twelve groups, exit 0, 28.73s build and 0.34s tests. Final
strict library/test Clippy passed, exit 0, 19.36s. The earlier twelve existing
heartbeat and eight membership passes remain applicable; neither unchanged
suite was needlessly rerun after this observation-only correction.

Final owned checks passed:

```sh
rustfmt --check --edition 2024 --config skip_children=true server/src/cluster/heartbeat.rs server/src/cluster/heartbeat/observation.rs server/src/cluster/heartbeat/observation/tests.rs
git diff --check -- server/src/cluster/heartbeat.rs server/src/cluster/heartbeat/observation.rs server/src/cluster/heartbeat/observation/tests.rs server/notes/DN-C03d.md
```

A direct byte check includes all four modified/untracked owned files and
confirms final LF and no trailing spaces/tabs/CR. Snapshot normalization removes
only the added declaration/reexports and restores the two removed item
allowances, then requires byte equality with the saved heartbeat.rs. It passed.
An initial preservation-check script used str.removesuffix, unavailable in the
local Python version; its compatible suffix-slicing correction passed. This
was a verification-script failure, not a Rust behavioral red.

No remaining unowned-interface blocker was found. Cargo/build and Rust
implementation ownership are explicitly RELEASED to the parent for NEW
independent specification review, then separate quality review.

## Parent quality correction: concrete drain identity

The independent quality review found that a registered Draining member with
`drain: None` is codec-valid. Two progressing loaded heartbeats with no drain ID
could pass the old optional-ID equality and produce DrainHeartbeat without its
required current drain association. This receipt alone supplied no idle or
termination authority, but its construction invariant was too weak.

The parent changed only observation.rs and observation/tests.rs. Receipt
construction now requires an existing member drain record and compares the
heartbeat against `Some(drain.id)`, retaining the existing Draining, complete-root,
boot, binding, sequence, freshness and configured-expiry checks. The codecs and
their recovery-state representation remain unchanged. No writer, loader,
membership or runtime code changed.

The focused regression uses independently roundtripped Control roots with and
without the durable drain, actual Fake-backed heartbeat writes and complete
load_heartbeat reads for sequence 100 then 101. Both cases establish actual
tracker progression and freshness before receipt construction. The concrete-ID
positive control checks the receipt's member and heartbeat IDs; the no-ID case
requires Conflict. Thus baseline or stale evidence cannot explain its refusal.
The fixture uses directly awaited in-memory storage futures, with no spawned
tasks, scratch files or external processes.

Pre-correction source copies are retained at
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-c03d-drain-id-i8ce8lqo/`.
Direct comparison confirms the narrow constructor correction and one added test.
After C05c explicitly released Rust/Cargo, the parent personally ran these
commands with the same four required environment settings and existing target:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::heartbeat::observation::tests::drain_receipt_requires_present_durable_drain_identity -- --exact
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::heartbeat::observation
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

The new regression failed behaviorally before correction at its missing-drain
Conflict assertion: exit 101, build 29.36s, test 0.01s. The positive control and
freshness assertions passed first. After correction all thirteen observation
groups passed: exit 0, build 29.02s, tests 0.37s. Strict library/test Clippy passed
in 20.80s. Each Cargo process exited before the next invocation. Owned rustfmt,
scoped Git whitespace and direct trailing-whitespace/single-final-newline checks
passed, including untracked files and this appended note. The earlier twelve
writer/loader and eight membership passes are reused without a later combined
rerun claim.

A fresh independent quality re-check remains required. No runtime, network,
periodic supervision, authenticated idle or termination guarantee follows.
