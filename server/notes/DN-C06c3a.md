# DN-C06c3a initial proof-bound stop claim and joint confirmation

Implemented only the initial claim/confirmation split of adopted A21 assignment 3.
C06b, C06c1 and C06c2 are reviewed prerequisites that passed both reviews, as
specified by the parent. Historical pending wording does not change that status.
The parent assigns a NEW independent specification review, corrections/recheck,
and then a NEW full quality review. No rubric or aggregate completion changed.

## Ownership and preservation

Exactly five owned paths:

- New `server/src/cluster/provision/stop.rs`.
- New `server/src/cluster/provision/stop/tests.rs`.
- `server/src/cluster/provision.rs`, private child declaration/reexports only.
- `server/src/cluster/drain/proof.rs`, only `PendingStopAllocation::confirm`
  visibility from `pub(super)` to `pub(crate)`.
- This new `server/notes/DN-C06c3a.md`.

Exact shared-file originals were saved before editing, with relative paths, at
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/dn-c06c3a-originals-c4nru6mw/`.
`comparisons.txt` and `changes.diff` there record final byte comparisons and the
exact shared-file changes. The entire original provision.rs is a byte-identical
prefix; only its stop declaration and reexports are appended. Reversing exactly
one visibility substitution reconstructs the original proof.rs byte for byte.
No existing test was edited. Fields, getters, other proof methods and proof
algorithms remain unchanged.

No applicable AGENTS.md was found in repository/ancestor discovery. Applied TDD,
entropy-reducer and unslop. Read the orchestration policy and c3a/c3b split,
distributed contracts, A21 common rules/assignment 3, A19/A10/A08, A04 callback
boundary, C06a/b/c1/c2, C05c/d and actual callback, shared delivery, proof,
allocation, guard, heartbeat, submission and Fake APIs.

Preserved shared main and existing modified/untracked work. No delegation,
branch, worktree, commit, push, network/cloud/paid action, secret/.env/profile
access, dependency/download/Cargo change, new target or target cleanup occurred.
Cargo used existing server/target. No delivery, callback, reconcile, store,
allocator, controller, heartbeat, member, state, auth, runtime, contract, design,
rubric or other-note edits were made.

## Private APIs and behavior

`claim_stop(root, owner, intent_id, ConfirmedDrain, invocation_id, now_ms, proposal)`
returns opaque `StopClaim`. It validates the complete root through delivery::base,
requires the held managed owner, and accepts only Pending/attempts=0 with no
prior delivery, error or outcome. It requires the exact retained stop/node/original
launch/resource/concrete incarnation and registered draining member/session.
Terminal/no-effect launch history and competing or contradictory retained stop
associations refuse. Original intent/global backoffs and fresh invocation/proposal
identity rules apply. A deadline supplies no termination evidence.

StopCommand::build reconstructs once from the exact retained root. The digest is
copied directly from that actual command before movement, never read from the
candidate delivery or supplied by a caller. ClaimCommand::Stop and existing
prepare_claim own checked attempts/max, exact delivery association, bounded
encoding and checked revision/change. PreparedClaim::into_parts returns the real
ProvisionCandidate and PendingClaim. The consumed proof's reviewed
prepare_stop_claim checks the base, exact permitted successor and actual command
digest. No generic transaction closure, duplicated command encoder or delivery
state machine was added. ProvisionCandidate field visibility is unchanged.

Only the stop state, attempts, current delivery and root revision/change change.
The complete restoration assertion and capacity comparison preserve every other
member, intent, controller, policy/admin, config, Template, Timing and scheduler
field, original deadlines and spent credits. There is no identity allocation,
new-admission or status gate.

StopClaim::into_parts consumes into the actual candidate plus PendingStop, which
privately owns the common PendingClaim and C06b PendingStopClaim. Existing
PreparedProvision/submit_control retains the captured token, one Match and at
most one reconciliation read under the original submission deadline.

PendingStop::confirm consumes ONE actual OwnershipOutcome. It explicitly borrows
both complete expected-root checks on that same value before confirmation. The
common pending command is confirmed by reference, then the proof consumes that
same outcome. A proof time failure drops the internally confirmed command.
Only complete Acknowledged/ObservedExact can yield ConfirmedStop. Conflict,
Unknown, wrong successors and identical change UUIDs with different complete
contents release neither usable command nor proof. There is no outcome clone,
second outcome or independently releasable pending authority field.

ConfirmedStop::into_parts consumes once into original command bytes, ReplyContext
and exact successor-bound ConfirmedDrain together. All fields are private; new
owners have no Clone/Serde/raw constructors/mutable accessors. Debug prints type
names only. Errors use existing sanitized StorageError categories. Item-level
dead_code allowances cover the later coordinator/consuming handoffs; one
unused_imports allowance covers the parent reexports, and too_many_arguments
applies only to the concrete claim function. Existing allowances remain.

## Tests and causal assertions

Thirteen new groups use real generation-2 codecs, memory-only worker signing,
actual reply signature/content/eligibility verification and the original outgoing
RequestContext/bind_reply. HeartbeatWriter publishes increasing sequences that
are actually loaded through Fake and consumed through Observations. Actual
OwnershipGuard acquisition, observation and paired-sample authorization are used.
No proof field, synthetic receipt setter, public fixture facade, duplicate module
include or storage architecture was introduced. Local setup patterns are reused
from C06c2 without copying its behavioral matrix or saturation test.

The positive chain executes idle-confirmation CAS, allocate_stop/PreparedDrain
CAS, PendingStopAllocation::confirm, fresh consumed check_live, claim_stop,
PreparedProvision CAS, joint confirmation, exact reload, consumed check_live and
fresh final authorization with the original guard generation. Original command
bytes are compared exactly; validate_stop_bytes and both original and released
contexts check the current delivery and digest. Context internals stay private.

A separate freshly authenticated round starts with an already retained Pending
stop, uses C06c2 StopPlan::Existing and its consuming extraction, then reaches
initial claim, joint confirmation and fresh final guard authorization. This
exercises C06b's fresh-round retained-stop path independently of carried allocation.

The gated lost-response test uses Fake's existing ReadGate and actual submission.
An applied claim with exact reconciliation produces ObservedExact and joint
release. While reconciliation is gated, an actual takeover CAS supersedes the
claim and makes its result Unknown, withholding both outputs. The lost-response
injection is disabled before takeover. Gate observation has a two-second bound;
results are recorded, the gate released, the original task settled within its
watchdog or aborted and joined, the gate removed and current root reloaded BEFORE
assertions. The original five-second submission deadline is retained. Cleanup
retains ownership after watchdog expiry. No task is detached, and no sleep is
used as causal takeover evidence.

Wrong successor and same-UUID/different-contents controls use actual loaded roots
wrapped as Acknowledged outcomes to challenge complete equality; those controls
are outcome-binding tests, not executed competing CASes. Separate explicit
Unknown/Conflict controls include the actual expected loaded current root but
still withhold release. Actual competing takeover/lost-response behavior is
established by the gated test above.

Independent codec-valid substitutions change owner/takeover, unrelated policy,
resource, incarnation, stop identity, node or original launch. The real typed
claim boundary rejects the old proof. Another codec-valid two-target root proves
that a proof for the first node cannot claim the second retained stop in the SAME
root. Multiple retained stops also refuse. Backoff, reconciling and old in_progress
state, prior pending delivery/error/attempts, failed stop, no-effect start,
revision overflow, retained invocation roles and proposal collisions are tested.
Attempt MAX on Pending refuses eligibility; this is not a checked-add overflow
claim. Initial attempts are zero and valid max_attempts is positive, so the
shared max/add checks are reused, with a positive max_attempts=1 boundary.

An overdue intent retains its original deadline through claim; later paired
samples beyond the original guard deadline deny final use. A separate actual
clock deadline expires the original one-second drain round after successful
claim CAS, and joint confirmation then withholds both outputs. This uses
timeout_at with a pending future, not fabricated proof fields or a reset round.
Negative Clone/Serialize/Deserialize checks and value-free Debug assertions cover
all three new owner types.

These tests prove component composition only. They establish no provider truth,
real callback entry, listener behavior, HTTP execution or termination.

## Commands, results and provenance

EVERY Cargo used this environment:

`CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0`

Every test used this exact form:

`cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib FILTER -- --test-threads=2`

Sequential runs, awaited to actual process exit before another Cargo, source
mutation or rustfmt:

1. `cluster::provision::stop`, `/tmp/dn-c06c3a-red.log`, exit 101.
   Compiled behavioral red, 0/1 passed. Real signed/heartbeat/idle/allocation CAS
   setup reached the rejecting claim stub and failed claim-is-ok. Temporary
   unused warnings belong only to this red stub/test stage.
2. Same filter, `/tmp/dn-c06c3a-green1.log`, exit 101. Compile-only fixture error:
   ReplyContext has no observation getter. Corrected to its existing public-to-
   cluster intent/association methods, without widening callback visibility.
3. Same filter, `/tmp/dn-c06c3a-expanded.log`, exit 0, 7 passed.
4. Same filter, `/tmp/dn-c06c3a-final-stop.log`, exit 0, 10 passed.
5. Strict Clippy below, `/tmp/dn-c06c3a-clippy.log`, exit 101, one collapsible_if
   diagnostic. Replaced the nested history condition with named booleans, with
   no new allowance. Both borrowed outcome checks also became explicit bindings.
6. Focused stop, `/tmp/dn-c06c3a-stop-green.log`, exit 101, 10 passed/1 failed.
   Fixture loop was extended without its two new match arms; unreachable! fired.
   Corrected the fixture arms. The actual clock-expiry group passed in this run.
7. Focused stop, `/tmp/dn-c06c3a-stop-final.log`, exit 0, all 13 passed,
   build 35.94s, tests 1.18s. No subsequent Rust source changes.
8. `cluster::provision::delivery::tests`, `/tmp/dn-c06c3a-delivery.log`, exit 0,
   all 15 passed. Covers preserved shared preparation/confirmation/start behavior.
9. `cluster::drain::proof::tests`, `/tmp/dn-c06c3a-proof.log`, exit 0,
   all 21 passed. Covers unchanged continuations and delayed-negative correction.
10. `cluster::drain::stop::tests::signed_idle_cas_then_reserved_stop_cas_preserves_root_and_live_authority`,
    `/tmp/dn-c06c3a-allocation.log`, exit 0, 1 passed.
11. `cluster::drain::stop::tests::retained_failure_and_exact_max_revision_converge_before_growth_checks`,
    `/tmp/dn-c06c3a-retained.log`, exit 0, 1 passed.
    These selected regressions cover cross-module allocation confirmation and
    Existing convergence without rerunning expensive unchanged saturation.
12. With the same environment,
    `cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings`,
    `/tmp/dn-c06c3a-clippy-final.log`, exit 0, 23.06s, no diagnostics.

Runs 8-12 were serialized by subprocess.run, which awaited each actual process
exit before starting the next command. All new assertions after the initial
claim red have green-only provenance; compile/fixture failures prove no missing
production behavior. These are separate focused runs, not a combined final suite
or universal failing-before claim. No blanket library/default/S3/runtime suite ran.

Formatting was restricted to owned Rust paths:

`rustfmt --edition 2024 --config skip_children=true server/src/cluster/provision/stop.rs server/src/cluster/provision/stop/tests.rs server/src/cluster/provision.rs server/src/cluster/drain/proof.rs`

Later formatting used the same options on only the changed owned files. Final
rustfmt --check uses those same four paths/options. Direct byte checks cover all
five owned paths, including all new/untracked files, requiring no trailing
whitespace and exactly one final newline. Scoped git diff --check, direct checks,
format checks and original-copy comparisons passed. Tracked Git diff alone
cannot establish preservation or whitespace for these untracked additions.

## Later obligations and release

C06c3b retains complete_stop/uncertain_stop, old in_progress takeover adoption,
fresh complete inventory retry, shared failure-code classification and
conservative backoff. C06d retains positive termination. No part of those flows
is implemented here and no credit is restored.

The later coordinator must retain candidate/pending/submission ownership through
cancellation and the original budget. Immediately before a single HTTP attempt,
it must consume check_live using actual current Observations/OwnershipGuard and
freshly authorize that same complete root and captured generation with paired
time. Any unrelated newer root or takeover requires a new authenticated round.
The original proof round and authority deadlines never extend through these CASes.
Callback entry independently requires A04's fresh durable authorization. Guard
renewal, submission/delivery supervision, bounded coordinator execution, actual
HTTP/probe handling and provider reconciliation remain later integration.

Rust implementation and Cargo/build ownership are explicitly RELEASED to the
parent. All initiated processes exited, including Cargo, strict Clippy, formatters,
the serial runner and every gated submission task. No owned process, gate or
async task remains running.
