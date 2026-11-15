# DN-C04b2c local ownership certainty

Implemented only the third accepted A10 mini on shared main. Read accepted A10
including FINAL corrections, distributed contracts, A05/A04 authorization
context, C04b1/C04b2a/C04b2b notes, and actual controller/store/state/Fake APIs.
No applicable ancestor or repository AGENTS.md was found. Applied TDD and
unslop. Parent retains independent specification and quality review.

## Delivered files and API

- `src/cluster/controller.rs` adds crate-private synchronous OwnershipGuard
  begin/finish/observe/check/invalidate and authorization_generation/authorize.
  OwnershipStamp carries boot UUID and checked generation. The caller supplies
  a fresh process boot UUID once. No restart authority is reconstructed.
- Private OwnershipSample, OwnershipObservation, OwnershipProposal and
  OwnershipEvidence enforce the preparation boundary. Fields are private.
- `src/cluster/controller/guard_tests.rs` adds explicit-sample guard cases.
  Two existing private fixture helpers gain sibling-test visibility only.
- `src/cluster/store/tests.rs` adds four combined guard/store groups using the
  existing before/after Match gates, Fake and owned bounded task helpers.
- `src/cluster/store.rs` only removes obsolete PreparedOwnership struct and
  OwnershipOutcome enum dead_code allowances and adjusts the removal comment.
  CAS behavior is unchanged. The impl allowance remains because submit still
  awaits runtime callers. OwnershipChange's enum allowance also awaits runtime
  construction. Guard item/impl allowances have explicit runtime removal
  obligations. No module-wide suppression or dummy runtime caller was added.
- This completion note. No other file was changed for this mini.

## Lock boundary and caller obligations

Outside the mutex, sample Instant/SystemTime and convert to checked Unix
milliseconds. OwnershipObservation::new validates and bounded-encodes a root,
then hashes the complete encoding with existing SHA256. Revision, change UUID,
digest and complete controller tuple are the retained identity. The guard holds
no backend token, credential, serialized root or configuration body. Its types
provide no Debug implementation that could expose such data.

Outside the mutex, OwnershipProposal::new prepares the validated base identity,
complete expected candidate identity and pre-I/O checked monotonic deadline.
Candidate/time errors remain in the wrapper so begin can first observe the
base, reject a still-active operation, advance generation and close certainty.
On a local precomputation error begin returns that sanitized error with no
occupied slot. No storage has occurred. This wrapper performs no CAS and does
not construct the consumed store submission.

Under the short mutex, begin binds the operation to the exact precomputed
base/change, explicit proposal UUID, original sample, expected candidate and
boot owner/epoch. Outside it, proposal.prepare consumes the LoadedControl via
the existing PreparedOwnership API, checks the base's complete identity, and
uses exactly the original Unix sample, requested change and explicit proposal
UUID. It never generates another UUID or substitutes acknowledgment wall time.
The caller submits outside the mutex. Any preparation/submission error or
cancellation after successful begin must complete that original stamp.

Convert store outcomes to OwnershipEvidence outside the mutex, including all
encoding/digest work. Pass the Result and paired completion sample Result into
finish under the mutex. Obsolete stamps are checked before either Result and
cannot clear certainty, release another slot, advance generation or replace
high-water state. Every active terminal completion releases its own slot.
Only an exact whole-candidate acknowledged acquire/renew for this boot can
install certainty. Release, read-back, conflict, uncertainty and errors cannot.

observe compares fixed-size identities under the mutex. Older roots conflict
without mutation; equal-revision disagreement invalidates with InvalidCatalog.
Every newer observation discards pending completion, even for the same tuple.
Already installed certainty survives unchanged-controller admin/member/intent
updates with its original anchor/deadline. Observation never restores authority.

For later A04, capture authorization_generation under the mutex, read/observe a
fresh root and validate the command outside it, then authorize under the mutex
with that generation, exact precomputed observation and fresh paired sample.
A superseded root fails even when unchanged-controller progress preserved the
generation. There is no endpoint or intent authorization implementation here.

Current process-wide failures use invalidate. Read/validation failures before
successful begin require caller handling through that path. Ordinary closed
admission during renewal denies permission without cancelling its pending
operation; actual sample/clock failures invalidate it. Runtime must supervise
and finish submitted tasks and cancellations, stop admission when closed and
never retain this mutex across any await. No timer or controller loop is added.

## Time and ordering

The deadline is start Instant plus lease, checked before I/O. Finish/check use
strict monotonic and Unix expiry and signed wall-versus-monotonic elapsed from
the original anchor. Drift greater than 2000 ms closes certainty; exactly 2000
ms is tolerated. Monotonic precision is retained in nanoseconds for this
comparison, avoiding truncation above the threshold. Backward Instant elapsed,
pre-epoch wall samples, exhausted conversions/additions and generation capacity
fail closed with existing sanitized errors. Generation exhaustion permanently
closes the guard without reset or stamp reuse.

## Executed red and green evidence

Every Cargo invocation ran sequentially in the existing target with this prefix
and flags, with no default features or network resolution:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::controller
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::controller::guard_tests
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::controller::guard_tests::closed_admission_during_renewal
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::store::tests
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
```

Initial behavioral red ran the first guard test against temporary closed stubs:
1 failed, exit 101, because exact acknowledged acquisition did not install.
The first implementation passed all 13 controller groups in 4.60 s. Initial
combined store tests passed all 13 groups in 0.47 s.

An added submillisecond drift regression failed with Owner instead of
Unavailable, 10 passed/1 failed, exit 101. Nanosecond comparison corrected it.
A transient variable-renaming compile error exited 101 and was corrected.
A member fixture then failed existing root validation because its intent still
named the old resource. Updating both identities corrected that fixture rather
than weakening validation. A separate renewal-admission regression failed
because checking closed certainty discarded the pending stamp. Its fix keeps
ordinary closed admission separate from clock failure.

Final controller run passed 18 groups, exit 0, in 5.47 s, including five existing
candidate regressions and thirteen guard groups. Final store run after adding
the original-sample/UUID preparation helper and actual preparation-failure case
passed 14 groups, exit 0, in 0.48 s. It exercises the helper through real Fake
CAS, including acknowledgment loss for acquire/renew/release, distinct fresh
renewal recovery, gated old acknowledgment after recovery or observed takeover,
late error/cancellation, newer slot preservation, and actual preparation failure
followed by successful retry. Tasks are bounded/joined or aborted/joined; no Fake
mutex crosses await and no new sleep supplies evidence.

A final new explicit actual forward/backward wall-step test passed alone, exit
0, in 0.01 s. It checks finish and check at both signed directions, exactly
2000 ms and 2001 ms. Its command used the same prefix/flags with filter
`cluster::controller::guard_tests::actual_forward_and_backward_wall_steps`.
There are now nineteen controller tests including existing candidate tests.

Guard cases also cover exact expiry during I/O, unchanged-controller progress
without deadline extension, same revision/UUID with changed content, candidate
UUID/content mismatch, older observation, pending invalidation on same-tuple
progress, superseded authorization reads, forward/backward wall steps and
exact tolerance, cumulative retained-anchor drift, pre-epoch/unrepresentable
wall time, backward Instant, checked timestamp/Instant capacity and permanent
generation exhaustion. Existing codec/CAS matrices were reused.

Strict no-default library Clippy passed with -D warnings, exit 0, in 8.28 s.
Owned rustfmt/check and whitespace checks include untracked files. No broad
suite, repeated passing run without new behavior/failure, dependency change or
fetch, target deletion, default AWS build, secret read, cloud call, delegate,
branch/worktree, commit or push occurred. Shared unrelated work was preserved.

A11 is adopted. Current validated Config is preserved; C08c still owns the
pending generation-2 codec/fixture amendment. This guard neither interprets
that amendment nor persists any state. It does not cancel entered effects,
provide distributed expiry fencing, prove worker drainage/readiness, or supply
real TCP/provider/runtime evidence. Independent reviews remain parent-owned.
Cargo ownership is released at delivery after the owned final checks.

## Parent correction after quality review

The quality reviewer found that check returned closed-admission Unavailable
without checking an active renewal's time anchor. A sampled wall-clock jump
could be ignored, followed by an otherwise exact acknowledgment after the clock
returned. The original pending stamp could then install certainty.

The parent added pending_renewal_clock_failure_makes_later_acknowledgment_obsolete
before changing production. Its first run failed behaviorally: begin at Unix
10000 ms, check at monotonic +1 ms / Unix 13002 ms, then acknowledge at +2 ms /
Unix 10002 ms returned true instead of false. Exit 101, one failed group, 0.01 s.

Established certainty and pending operations now share the same private
check_ownership_time calculation. A pending operation retains its original
anchor, monotonic deadline and proposed Unix deadline. A release has no proposed
Unix lease but still checks its anchor and monotonic deadline. A real clock or
deadline failure invalidates the pending stamp. Ordinary closed admission with
a valid sample still denies authority without cancelling the pending operation.
No clock observation installs authority or changes the original anchor.

The regression covers forward/backward discontinuity, the inclusive two-second
tolerance in both directions, exact monotonic expiry, and independent Unix
expiry within the drift tolerance. After invalidation an exact old acknowledgment
is obsolete, a returned clock remains insufficient, and a distinct acknowledged
proposal can establish new certainty. These are explicit-time local guard
assertions; storage application remains covered separately by the existing Fake
CAS tests.

All commands used CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0
CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 and the existing target:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::controller::guard_tests::pending_renewal_clock_failure_makes_later_acknowledgment_obsolete
Red as described above.
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::controller
Exit 0, all 20 controller groups passed, 4.34s.
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::store::tests
Exit 0, all 14 store groups passed, 0.47s.
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
Exit 0, 8.90s.
rustfmt --edition 2024 --config skip_children=true --check server/src/cluster/controller.rs server/src/cluster/controller/guard_tests.rs
Exit 0. Scoped git diff --check and direct whitespace/final-newline checks passed,
including these untracked Rust files.
```

Changed only controller.rs, controller/guard_tests.rs, this evidence note and
the parent rubric. No storage, runtime or public interface changes. Fresh quality
re-check remains required. Cargo/build ownership is released after these checks.
