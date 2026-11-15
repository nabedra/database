# DN-C05b managed launch reservation and publication

Implements only adopted DN-A19 mini 2. C05a and C03b are delivered dependencies
with parent-reported independent review passes. A19's older pending-C03b text
is superseded. Applied test-driven-development and unslop. Inspected the shared
contracts, rubric orchestration/testing/C05/F10/F12/F13, A19 common rules/mini 2,
relevant A05/A08/A10/A16/A11, C05a/C03a/C03b, C04b1/C04b2a/b/c, C08c1/c2,
closed DN-test-lints and actual generation-2 codecs, controller observation,
callback, membership/store and shared Fake interfaces. No applicable AGENTS.md
was found. C03c observation/runtime, R04 and later C05 minis are not dependencies.

## Owned files and snapshots

Only these files were changed or created:

- `server/src/cluster/provision.rs` and private `provision/tests.rs`.
- `server/src/cluster/store/provision.rs` and private `provision/tests.rs`.
- Narrow declaration in `server/src/cluster/mod.rs` and private declaration plus
  crate-private reexport in `server/src/cluster/store.rs`.
- This note.

The two shared files were copied before editing to
`/tmp/meshdb-c05b-snapshots.kPKIPL/mod.rs` and
`/tmp/meshdb-c05b-snapshots.kPKIPL/store.rs`. Their original bodies are preserved.
The initial shared checkout status was recorded at `/tmp/c05b-initial-status.txt`.
No other source, Cargo/lock, rubric or design note was edited. No delegation,
branch/worktree, commit/push, target cleanup, dependency addition/fetch, .env,
real secret/profile inspection, network, cloud or paid call occurred.

## Exact APIs and invariants

All entry points below are crate-private:

```rust
managed_capacity(&Control) -> Result<u64, StorageError>
allocate_start(&Control, Owner, Uuid, Uuid, u64, Uuid)
    -> Result<ProvisionCandidate, StorageError>
PreparedProvision::prepare(LoadedControl, ProvisionCandidate)
    -> Result<PreparedProvision, StorageError>
PreparedProvision::submit<B: ObjectStore>(self, &Storage<B>, Instant)
    -> Result<OwnershipOutcome, StorageError>
```

Allocation arguments are base, exact owner, node UUID, launch UUID, Unix
milliseconds and proposal UUID. No UUID generation, clock sampling or I/O occurs.
Complete bounded base validation precedes held-owner and eligibility checks.
Managed mode, desired deficit and scheduler backoff are required. Revision and
readiness timestamp additions are checked. One node and one start are inserted
in sorted order, with the stored Template capacities/fingerprint, unregistered
starting phase, null resource/boot/session/endpoint/drain/delivery/outcome/error,
empty retirements, zero attempts and created/retry timestamps equal to now.
The entire resulting root passes check_new_admission, including its new credit.

Capacity counts each retained managed node/launch once. A matching failed start
with not_invoked/terminated, or a matching completed/failed stop with terminated,
is the only release witness. Validation checks the exact cross-record binding.
Ready resources do not count twice with their retained starts. Pending,
in_progress, reconciling, overdue/unregistered, lost, draining and phase-only
stopped records remain charged. A failed/not_invoked stop consumes the first-stop
reservation but releases no resource capacity. Codec validation cannot establish
the historical truth of terminal witnesses. Inventory absence, elapsed time and
stop callback acceptance are never substituted for termination evidence.

New node/launch UUIDs differ from each other and every retained identity role,
including root cluster/change/admin/controller IDs, nodes, launches, current and
retired incarnations, sessions, drains, intent requests/targets and delivery
invocations/owners. Nothing is pruned. Proposal must differ from current change;
the caller retains broader fresh-proposal obligations because historical
proposal UUIDs are not all retained. Configuration, Template, Timing, complete
controller tuple, independent policy revision, admin history, scheduler and all
unrelated nodes/intents remain equal. Errors use existing Conflict,
InvalidCatalog and LimitExceeded categories with the existing codec mapping.

ProvisionCandidate is opaque, non-Clone and must-use. Only provision and its
children construct its private observation/Owner/Control. Consuming
`pub(super) into_parts` returns exactly those three values to store. There is no
raw Control/bytes constructor, arbitrary transaction closure or token accessor.
Later minis reuse this concrete candidate. PreparedProvision privately captures
the LoadedControl token, compares the complete OwnershipObservation and held
owner, preserves the exact controller tuple and validates/bounded-encodes the
result. Debug prints only PreparedProvision. Item-specific allowances identify
pending runtime callers; there is no blanket module lint suppression.

Consumed submit delegates directly to C03b's existing private submit_control.
It makes one captured-token Match and at most one reconciliation GET, sharing
the original absolute deadline. Only whole candidate and UUID equality confirms
ObservedExact; a later root remains Unknown. No history lookup, retry, new
identity/deadline, second writer or callback authority is introduced. Timeout,
unknown outcome, dropping or cancelling cannot imply rollback.

## Verification and provenance

Every Cargo command below used existing server/target and this exact prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Every invocation used `--locked --offline --no-default-features`. Each process
was awaited through exit before the next Cargo process started.

The first two pure tests compiled against placeholders. Atomic allocation
failed with unexpected Conflict; an independently codec-constructed pending
launch failed with Ok(0) instead of Ok(1). Both are behavioral reds, exit 101,
40.35s build and 0.01s tests. Temporary placeholders emitted an unused observation
import and an unused fixture helper warning. The expanded pure run passed six
of seven groups, exit 101, 32.51s build and 0.82s tests. The failing identity
assertion incorrectly expected a no-longer-retained historical change UUID to
be rejected. It was corrected to test the actual current UUID. This was a test
fixture correction, not a production bug fix or extra behavioral-red claim.

Four store groups then compiled and failed at valid preparation against its
rejecting placeholder, exit 101, 31.42s build and 0.02s tests. After preparation
implementation, all 11 pure/store groups passed, exit 0, 35.30s build and 0.83s
tests. Submission delegates the existing implementation from the outset; no
separate submission behavioral red is claimed. Expanded distinctions and later
strengthened assertions have green-only provenance. No compile-error red occurred.

Commands executed to this point:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::provision::tests -- --test-threads=2
# Initial red, then expanded run with the fixture failure described above.
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::store::provision::tests -- --test-threads=2
# Preparation red.
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib provision::tests -- --test-threads=2
# Combined green; final strengthened run recorded below.
```

Tests use real generation-2 Control/member codecs and only crate::test_storage's
Fake declaration. Seven pure groups cover capacity union, independent ambiguous
phases and terminal witnesses, exact initial fields/sorting/whole-root
preservation, no deficit/zero/external/backoff/owner eligibility, retained ID
roles, independent valid revision/time exhaustion and augmented admission.
The saturated root retains 1536 nodes/starts, 513 terminal failed starts and
1023 charged launches/credits. Its 4095 augmented slots admit the base but reject
one more three-slot allocation without changing the base.

Four store groups test complete-observation/held-owner preparation, two
allocations captured from one token, applied lost response with exact versus
unrelated later readback, and old allocation fenced by successful takeover.
Fake PUT/GET counts and captured Match strings are asserted, with final durable
root/token state. Before/after-application gates establish causality, use finite
five-second watchdogs and release before settling owned tasks and asserting
outcomes. A settlement timeout aborts and joins the owned task. No sleeps or
detached effects are used. C03b's detailed shared-helper error matrix is reused.

## Caller obligations and limits

Production allocation is not enabled. Runtime must encode the complete actual
public-status response envelope under 1 MiB before new allocation, using the
later status owner. No caller boolean or alternate status encoder is supplied.
Unknown/conflicting inventory remains a fresh complete reconciliation gate,
not guessed extra or released capacity. Runtime must retain guard generation,
exact fresh complete observation and live owner certainty through final
submission/authorization, supervise pending work through cancellation and
forbid hidden backend retries. Metadata acknowledgments/readback grant no live
callback authority. Run bounded synchronous clone/encoding/observation work on
the existing finite coordinator executor outside short locks. Inputs and sets
are bounded by existing record limits; no peak-RSS measurement is claimed.

Retained recovery and reserved first-stop conversion remain later safe progress
paths even when new admission or projected status is full. This slice adds no
new-admission requirement to those paths and changes no existing reservation
logic. Positive termination/drain proof, provider idempotency, actual callback
HTTP authorization, runtime loops, startup and process acceptance remain later
assignments. These Fake tests prove in-process metadata behavior only. There is
no provider, projected-status, runtime or aggregate C05/F10/F12/F13 completion
claim. Parent must assign fresh specification review followed by quality review.

## Final checks

The strengthened combined focused run passed 11 tests, 233 filtered, exit 0,
34.72s build and 1.06s tests. No production behavior changed afterward.
Affected regressions were limited to the consumed store boundary and the
control codec's first-stop/admission guarantees:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::store:: -- --test-threads=2
# 32 passed, 212 filtered; exit 0; 1.43s build, 0.90s tests.
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_state -- --test-threads=2
# 10 passed; exit 0; 16.92s build, 2.83s tests.
```

The first covers existing ownership/guard, membership and admin publication
through the unchanged shared helper. The second includes actual fleet first-stop
conversion, retained growth and admission saturation, so full admission does
not become a retained-progress gate. No full/default/S3 suite was run.

Owned formatting passed, exit 0:

```sh
rustfmt --check --edition 2024 --config skip_children=true server/src/cluster/provision.rs server/src/cluster/provision/tests.rs server/src/cluster/store/provision.rs server/src/cluster/store/provision/tests.rs server/src/cluster/mod.rs server/src/cluster/store.rs
```

A direct seven-file byte check including untracked files passed with exactly
one final LF and no trailing spaces/tabs. Removing the new declaration/reexport
text from both shared files reproduces their pre-edit snapshots byte-for-byte.

Strict linting passed without diagnostics, exit 0, 19.51s:

```sh
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

The required Cargo environment prefix above applied to this invocation too.
The final scoped whitespace check includes this completed note and untracked
owned sources. All Cargo processes have exited; no owned tasks remain running.

Cargo/build and Rust implementation ownership are explicitly RELEASED. Parent
then assigns fresh independent specification review followed by quality review.
