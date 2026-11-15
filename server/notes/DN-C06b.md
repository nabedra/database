# DN-C06b closed drain proofs

Implemented adopted DN-A21 assignment 2 only. This delivery awaits the parent's
NEW independent specification review, fixes/recheck, NEW full quality review and
rubric decision. No aggregate C06, runtime, HTTP, provider or termination
acceptance is claimed.

## Owned files and snapshot

- New `src/cluster/drain/proof.rs` contains closed proof composition and the two
  explicit stop continuations.
- New `src/cluster/drain/proof/tests.rs` contains private real-codec, signed auth,
  live guard, loaded heartbeat and Fake submission tests.
- Existing `src/cluster/drain.rs` adds only `pub(crate) mod proof` and removes the
  obsolete dead-code allowance on the now-consumed DrainBinding. Its existing
  transition bodies, candidate handoff and tests are unchanged.
- This new delivery note records component evidence and caller obligations.

The pre-edit drain.rs copy is outside the repository at
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-c06b-_svzsczh/drain.rs`.
The final exact-content comparison permits only the two changes above. A unified
diff and comparison record are stored beside that snapshot. This comparison
establishes present content, not provenance of the other shared untracked work.

No contracts, rubric, Cargo, dependency, auth, admission, heartbeat, store,
controller, C05 or runtime files changed. No delegation, branch, worktree,
commit, push, download, network/cloud/paid operation, secret/profile/.env access,
new target directory or target cleanup occurred. Existing server/target was used.
TDD and unslop were applied; entropy guidance favored existing candidate/store
plumbing and two explicit continuation predicates over a generic successor API.

## Exact private APIs

`DrainRound::new(root, owner, DrainBinding, Auth, initial_bytes, guard, sample)`
creates and owns the actual RequestContext through for_send against the complete
root. It requires the retained registered draining node, exact request target,
drain, held owner and live authorization generation. `context()` borrows that
original outgoing context. `bind_reply(self, EligibleReply, CheckedReplyContent)`
consumes it through the actual A18 into_reply_handoff and returns opaque
DrainRoundReply, which owns both the round and actual ReplyHandoff. This refines
the design's separate round/handoff arguments to prevent substitution even by
an otherwise identical separately allocated context. No auth interface change
or arbitrary reply/receipt constructor was needed.

`check_drain_reply(root, owner, DrainRoundReply, live_api_sender, DrainHeartbeat,
observations, guard, sample)` consumes both closed handoffs. It checks the live
API boot/key, complete root, held owner/generation, original association, positive
signed Drain reply and current C03d receipt. It returns Pending or
Positive(PositiveDrain). Barrier/sequence/content/authentication parsing remains
owned by A17/A18. The signed idle bit additionally requires a matching actual
loaded heartbeat, configured freshness, sufficient positive sequence,
accepting=false and all four occupancy metrics zero. A refusal calls the actual
Observations::read_failed operation before returning Pending, invalidating prior
receipts independently of durable clear_idle. PositiveDrain retains the original
ReplyContextBinding, round and opaque heartbeat receipt.

`confirm_idle(root, owner, PositiveDrain, proposal, observations, guard, sample)`
rechecks live evidence and returns IdlePlan::Existing(ConfirmedDrain), or
Change(DrainCandidate, PendingDrainConfirmation). Only ack_seq and confirmed_idle
change. Exact durable acknowledgement convergence precedes proposal/revision
arithmetic, including MAX revision. PendingDrainConfirmation owns the entire
bounded expected root and original proof. `confirm(self, OwnershipOutcome)`
accepts only whole-root Acknowledged or ObservedExact. OwnershipOutcome is the
actual existing shared store result, corresponding to the handoff's
ControlWriteResult terminology. Conflict/Unknown never yield a proof.

`ConfirmedDrain::check_for_stop(&self, root, owner, generation)` checks its exact
complete root and unchanged draining member/ack, held owner, generation and
original receipt time limit. A refused other root does not mutate the proof.
`check_live(self, root, owner, observations, guard, sample)` additionally consumes
and checks current C03d continuity and actual guard authorization. It returns the
same proof only on success.

`prepare_stop_successor(self, DrainCandidate)` is visible within drain. It accepts
only the exact first eligible pending stop insertion, checked revision/change,
retained node/launch/resource/incarnation, initial fields, checked deadline and
reserved credit conversion. Restoring that one insertion and root identifiers
must reproduce the entire base. It returns the unchanged candidate plus
PendingStopAllocation. Its consuming confirm accepts only the whole expected
Acknowledged/ObservedExact root and binds the proof to that successor.

`prepare_stop_claim(self, base, successor, command_digest)` is cluster-visible for
later provision/stop. It selects the unique exact retained stop from the proven
base, including a stop already present when a fresh round began. A carried
allocation stop must agree. Only pending/reconciling to in_progress, checked
attempt increment and fresh invocation/current owner/epoch/exact supplied digest
delivery may change, plus checked root revision/change. All other fields must
reproduce the entire base. PendingStopClaim owns the expected bounded root and
original proof. `matches_outcome(&self, &OwnershipOutcome)` supports the later
command/context wrapper's borrowed check; consuming `confirm` releases the proof
only for the same whole root. No storage outcome is cloned.

All proof fields remain private. Proof types are non-Clone, non-Serde, with
value-free Debug. Errors use existing sanitized StorageError categories. Item
allowances identify pending C06c/runtime consumers; the two owned plan enums
explicitly allow their required direct owned large variants. There is no blanket
module allowance or public facade.

## Live checks and later caller obligations

Use paired OwnershipSample values and one current Observations instance. Missing
reply, rejected authentication/content, failed/missing load or cancellation before
composition must consume/drop the original round and call
Observations::read_failed(binding, now) before any retained local proof is used.
Feed every later actual heartbeat through C03d ordering checks. Never replace the
Observations instance to recover an old proof. matches_drain is called at reply,
idle construction and consuming check_live boundaries. Failed reads, regressions,
staleness, nonzero samples and authenticated idle:false invalidate existing
receipts. A later zero cannot restore their generation. A new round can progress
with fresh loaded evidence; a new owner needs fresh sequence advancement.

The only permitted root continuations are the fully checked idle confirmation,
first-stop insertion and stop claim. They preserve the original heartbeat receipt
and continuity limits; they do not replace its observation or reset its clock.
Reconciling Observations to a changed complete root can conservatively invalidate
that original receipt, requiring a fresh round. No arbitrary observation setter
is provided. Receipt matching and successor-root matching remain separate checks
through these explicit continuations.

Run synchronous preparation, complete encoding/hashing and proof checks on the
bounded coordinator outside short shared guard locks. These synchronous APIs
borrow the actual guard; callers must supply exclusive coordinator access, not
wrap the whole operation in a short authority mutex. The final action path must
prepare its complete OwnershipObservation outside the lock, freshly check the
root/proof and live continuity, then perform guard.authorize with the captured
generation and a fresh paired sample immediately before acting. Metadata
acknowledgement never calls guard.finish or installs certainty. Lease extension
or unrelated root progress cannot independently continue a proof.

C06c still owns action eligibility, stop allocation/claim APIs and actual typed
StopCommand construction. Its claim wrapper must supply command_digest borrowed
from that validated immutable command, never copy it from an unchecked delivery.
It must perform the borrowed outcome check before consuming that same outcome to
release command/context and proof together. Backoff/inventory eligibility and
final authorization remain required. No command, HTTP attempt, scheduler,
provider action, listener or process shutdown was implemented here.

## Tests and TDD provenance

Tests authenticate complete small wire bodies with WorkerSigningKey, verify_reply,
check_reply_authority, check_reply_content and consuming A18 handoff. Heartbeat
receipts come only through actual publication, complete Fake-backed loading and
C03d drain_heartbeat. Guards acquire through real proposals and acknowledged
PreparedOwnership submissions. The existing R04c3d reviewed real owner/receipt
composition remains prerequisite evidence; these tests do not duplicate its
worker supervisor or claim new provider settlement evidence.

Observed behavioral reds were the initial whole-root confirmation stub, the
valid signed/loaded positive baseline against the rejecting check stub, later
idle:false failing to invalidate an earlier positive, and a fresh round unable
to claim an already retained stop. Each failed at the intended assertion before
its implementation/fix. Other later assertions are green-only coverage, including
the initial stop-continuation predicates. No per-function universal TDD claim is
made. Fixture failures are recorded separately below.

The final 20 proof groups cover positive composition; independent live API
boot/key, request allocation, owner/generation/root, freshness, failed read and
regression refusal; all four loaded occupancy metrics; nonzero/failure followed
by zero with old-proof refusal and independent new-round progress; idle:false;
post-takeover advancement; whole unrelated-root preservation; exact MAX
convergence and changed-MAX overflow; opacity/redaction; real competing and
lost-reply metadata outcomes; complete stop allocation/claim continuation and
fresh guard authorization; mismatched successor/attempt/owner/invocation/digest;
whole-root including equal UUID/different content; and retained-stop re-probing.
Existing A17/A18 wire/signature/status and C03d codec matrices are reused.

The lost-reply test causally gates the actual reconciliation GET after Fake has
applied the original Match. It releases every gate, retains the original
submission JoinHandle through watchdog expiry and awaits it before assertions.
It distinguishes ObservedExact from a superseding-root Unknown without resending.
All original Fake effects settle. These memory-only fixtures create no worker,
provider, scratch or reaper resources needing dual-fixture teardown. No task is
detached and no protected/persistent resource is created.

## Commands and results

EVERY Cargo invocation, including failed runs, had this exact environment and
common argument set. All processes exited before the next Cargo invocation or
source/format mutation. Commands were sequential, with existing server/target.

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
--manifest-path server/Cargo.toml --locked --offline --no-default-features
```

The following table gives each command's remaining arguments and log suffix.
Each test command was `cargo test` plus the common arguments above, followed by
the listed arguments. Logs are `/tmp/meshdb-c06b-<suffix>.log`.

| Suffix | Remaining arguments | Exit and result |
| --- | --- | --- |
| red | `--lib whole_root_confirmation_accepts_actual_loaded_ack` | 101, fixture compile errors: method import/backend wrapper |
| red2 | same | 101, intended whole-root behavioral red |
| baseline-red | `--lib cluster::drain::proof` | 101, 1 passed, reply canonical-header fixture failed |
| baseline-red2 | `--lib authenticated_loaded_positive_baseline` | 101, intended rejecting-stub behavioral red |
| green | `--lib cluster::drain::proof` | 0, 2 passed |
| matrix | same | 101, 6 passed, wrong heartbeat fixture object path |
| continuation-red | same | 101, 10 passed, intended idle:false invalidation red |
| matrix-green | same | 0, 12 passed |
| final-proof | `--lib cluster::drain::proof -- --test-threads=2` | 0, 17 passed |
| final-drain | `--lib cluster::drain -- --test-threads=2` | 0, 24 passed |
| regression-cluster-store-drain | `--lib cluster::store::drain -- --test-threads=2` | 0, 4 passed |
| regression-reply_handoff | `--lib reply_handoff -- --test-threads=2` | 0, 5 passed |
| regression-cluster-heartbeat | `--lib cluster::heartbeat -- --test-threads=2` | 0, 41 passed |
| regression-controller-guard_tests | `--lib controller::guard_tests -- --test-threads=2` | 0, 15 passed |
| retained-red | `--lib fresh_round_can_continue_an_existing_retained_stop` | 101, intended retained-stop behavioral red |
| final-drain2 | `--lib cluster::drain -- --test-threads=2` | 0, 25 passed, including 20 proof and 5 unchanged C06a groups |

Both strict commands were `cargo clippy` plus the identical environment/common
arguments, then `--lib --tests -- -D warnings`. `clippy.log` passed, exit 0,
24.84s, before the retained-stop regression. The final `final-clippy.log` passed,
exit 0, 24.77s, after its fix and final 25-group run. Unchanged prerequisite
regressions were not repeated after the local retained-stop selection fix.

Owned formatting command, including the final check:

```sh
rustfmt --check --edition 2024 --config skip_children=true server/src/cluster/drain.rs server/src/cluster/drain/proof.rs server/src/cluster/drain/proof/tests.rs
```

Final owned rustfmt, scoped git diff --check, direct trailing-whitespace and
exact-final-newline checks including untracked files, and the exact drain.rs
snapshot comparison passed. Parent review and rubric updates remain outstanding.

All initiated Cargo, rustc, test, Clippy, formatting, shell and fixture tasks have
exited. Rust implementation and Cargo/build ownership are explicitly RELEASED to
the parent. No independent review result is claimed.

## Parent correction: delayed negative after idle confirmation

The NEW specification review found one P1: check_drain_reply returned early on
round/root/guard errors before invalidating Observations. An authenticated
idle:false reply retained against root R could arrive after idle confirmation
advanced the root to R'. Its round mismatch returned Conflict while the held
successor-bound ConfirmedDrain still passed check_live and guard authorization.
Local invalidation must not wait for a durable clear_idle write.

The parent changed only check_drain_reply in proof.rs and appended one grouped
test to proof/tests.rs. Pre-correction copies use repository-relative paths under
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-c06b-negative-elby9xpf/`.
drain.rs is unchanged and the original tests remain an exact prefix. Saved
comparisons establish current content relationships, not historical provenance.

Checks now produce an optional acknowledged sequence or an error inside one
bounded synchronous block. Every nonpositive result, including early round,
observation and guard errors, calls the existing Observations::read_failed before
returning. Successful invalidation preserves the original error or Pending
classification. Positive construction still requires all existing exact checks.
This adds no auth, heartbeat, store, controller, wire or public API change and
does not accept positive authority from a superseded association.

The new regression creates actual signed positive and delayed negative rounds
against R, submits idle confirmation through PreparedDrain/Fake, observes the
acknowledged successor in the real guard, and establishes a successful live check
before processing the delayed reply. It records immediate receipt invalidation
and then consumes the successor proof through check_live. Two independent runs
cover immediate use and use after a newer actual zero heartbeat is published,
loaded and accepted as fresh. Both still have valid fresh guard authorization;
the failure therefore comes from the revoked drain evidence. All operations are
awaited and both runs finish before their collected observations are asserted.

Parent execution used the required environment on every Cargo command:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib delayed_idle_false_invalidates_confirmed_successor_before_and_after_later_zero -- --nocapture
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::drain -- --test-threads=2
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

Logs: `/tmp/meshdb-c06b-negative-{red,red2,green,drain,clippy}.log`.
The first compiling red reproduced the immediate continued-proof defect but its
later-zero fixture only reconciled the stored sample. That fixture was corrected
to advance the actual writer before the second red; no production change had
yet occurred. The second red (build 45.28s, test 0.13s) had both valid controls and
reported missing invalidation in both runs, with immediate check_live also
incorrectly succeeding. After the fix the group passed (build 36.99s, test 0.09s).
The full drain filter then passed all 26 groups (build 0.88s, tests 0.59s), followed
by strict library/tests Clippy in 23.90s. No compile failure or additional broad
regression rerun is claimed for this correction. The prior 65 unchanged
store/auth/heartbeat/guard regression results remain implementer-reported evidence.

Owned rustfmt, direct whitespace/final-newline and snapshot checks passed. Every
Cargo and formatting process exited before the next mutation or Cargo command;
all initiated processes exited before handback. Rust/Cargo ownership is released
for a NEW specification re-check, then full independent quality review. C06b and
aggregate/runtime/provider/termination completion remain unclaimed.
