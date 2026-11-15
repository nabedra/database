# DN-C03a pure membership candidates

Implements only adopted A16 mini 1 in new `src/cluster/membership.rs` and its
private `membership/tests.rs`. The parent supplies the module declaration.
Read shared distributed contracts, C03/testing rubric, adopted A16 including
parent corrections, A05/A07/A08/A10/A11, relevant A15/A17, reviewed codec,
controller/configuration notes and R03a2b1 possession implementation/accessors.
Applied test-driven-development and unslop. No applicable ancestor or owned-path
AGENTS.md was found. Shared work is preserved. No provisional eligibility code
is imported, edited or required by this module.

## Exact private interfaces

The three crate-private synchronous functions take `&Control`, exact `Owner`,
respectively `&EnrollmentProof`, `&ReadinessInput` or `&ExpiryInput`, and caller
proposal `Uuid`, returning existing `StorageError` categories:

- `registration_plan` returns `RegistrationPlan::Existing(Node)` or
  `RegistrationPlan::Change(MembershipCandidate)`.
- `ready_candidate` and `loss_candidate` return `MembershipCandidate`.

MembershipCandidate is non-Clone, with private `base: OwnershipObservation`,
`owner: Owner`, and `result: Control`. Only this module constructs it.
Consuming `pub(super) into_parts(self)` returns exactly those three values for
C03b's sibling store child through cluster visibility. There is no byte/token
constructor, token accessor or public facade. Debug prints type/category only.
Pending dead-code allowances are item-scoped and identify later consumers.

ReadinessInput has crate-private fields `cluster_id: Uuid`, `member: Node`,
`observation: OwnershipObservation`, `challenge: Uuid`,
`verified_challenge: Uuid`, `initialized: bool`. The complete member is the exact
probed node, including node/incarnation/session/endpoint/verifier/resource,
launch, capacities, phase and drain evidence. Exact member equality and complete
root observation equality associate the asserted probe with the proposed base.
The challenge is A07's UUID, and both challenge values must agree.

ExpiryInput has crate-private fields `cluster_id: Uuid`, `member: Node`,
`observation: OwnershipObservation`, `generation: u64`, `controller_boot: Uuid`,
`grace_anchor: tokio::time::Instant`, `last_sequence: Option<u64>`,
`last_advance: Option<Instant>`, and `sampled_at: Instant`. Controller boot uses
the same fresh UUID as Owner.process_id. Generation must be positive. Sequence
and last-advance must either both be absent or contain a positive sequence and
anchor. Using checked Instant addition, the sample must be at or beyond the
base Timing expiry interval after grace and, when present, last advancement.
A missing heartbeat uses the tracking/grace anchor without inventing a sequence.
The two anchors need not be equal or ordered relative to each other.

Both input types are plain caller assertions with value-free Debug, not
certificates. Matching fields and elapsed intervals are pure comparisons.
Neither function authenticates a probe, establishes initialized storage/catalog,
checks continuity, reads a heartbeat, or knows the live observation generation.

## Behavior and invariants

Every call validates the whole base via OwnershipObservation::new, then checks
held exact owner/epoch. Every change requires a distinct proposal, checked root
revision+1 and full bounded output validation. Complete cloning preserves
controller/config/template/timing/policy/admin/scheduler and every intent.
Unrelated members remain equal. Retained changes use reserved growth without
new-admission checks. Exact registration replay precedes proposal rejection and
counter arithmetic, so it succeeds unchanged at revision exhaustion, including
when its proposal equals the base change UUID. Owner checks still apply.

Managed registration requires exact cluster/node/request/launch/capacities,
retained start and persisted validated template fingerprint. Only in-progress
or reconciling with delivery and positive attempts, or completed/resource-known,
is eligible. First registration requires unregistered starting/draining. It
preserves state/drain, leaves resource unknown when unknown, and changes no
intent/delivery/outcome. Numeric launch/readiness deadlines are not eligibility
cutoffs. Exact starting/ready/draining retries compare every enrollment binding
field and preserve verification/drain/session/endpoint. Failed starts and
lost/stopped managed identities cannot register again.

External registration requires explicit External provisioning and null request.
New IDs get `external:<node_id>` and whole-candidate check_new_admission.
The required bounded projected-status encoder gate is a LATER prerequisite
before enabling new-admission routes. This module does not implement that
encoder and must not be treated as full new-admission authority.
Stable-ID replacement requires lost/stopped, different incarnation/session,
and room in permanent sorted retirement evidence. At 16 it refuses replacement.
It resets verification/drain, requires readiness again and keeps every retired
identity. A proof incarnation retired anywhere in the root is rejected.
Replacement is retained growth, including at saturated new-admission bounds.
There are no provider intents or autoscaling effects.

Readiness requires exact starting/registered, known resource, complete probe/root
association, initialized true and matching challenges. It changes only phase
and registration to ready/verified. Expiry requires exact registered binding
and supplied monotonic evidence. It changes only starting/ready to lost.
Draining is refused and remains available for authenticated C06 reconciliation.
There is no idle/termination/stopped proof, stop request or expiry of an
unregistered ambiguous launch.

## Evidence and commands

All Cargo commands use existing server/target and the exact environment:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

From repository root, commands after that prefix:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::membership::tests
```

Initial registration test compiled against a rejecting stub and failed at the
expected valid registration unwrap, 1 failed, exit 101. Expanded six groups
failed at missing registration, which is not separate readiness/expiry red.
After registration implementation, seven groups ran with readiness/loss still
rejecting: four passed and three failed at valid readiness, valid expiry and
expected revision-limit classification. This is their direct behavioral red.
The next seven-group run passed, 164 filtered, 4.67s after 33.76s compilation.
An intermediate unused fmt import warning disappeared when Debug was added.
No compile-red occurred. Later assertions and the eighth group have green-only
provenance; no separate red is claimed for every negative assertion.

Tests use real generation-2 Control decode/validate/encode and independently
assembled canonical enrollment text, signed with synthetic ring Ed25519 keys
and passed through actual verify_enrollment. No production signer or invented
proof constructor is used. Groups cover first/late/dispatched/completed/draining
registration, exact reply-loss recovery, independent eligibility conflicts,
ready/draining retry preservation, changed sessions/keys/endpoints/capacities,
external admission/terminal replacement/sorted retirement/exhaustion, strict
readiness association, phase-safe expiry, monotonic/grace/sequence assertion
failures, owner/proposal/revision boundaries and consumed private parts.

Saturation uses 1366 retained managed launches whose admission augmentation is
full while retained progress remains encodable, and 2048 external members where
another ID fails while replacement succeeds. Full record/encoded comparisons
cover nonempty admin head/sequence, independent policy revision, scheduler,
unrelated members, all intents and unchanged first-stop credits. Template
fingerprint corruption is necessarily rejected by the root codec; enrollment
has no separate template fingerprint claim. Valid baselines precede isolated
negative mutations. Synthetic stop preservation proves record retention only,
not historical C06 eligibility or provider truth.

Final focused checks and results are recorded below before ownership release.

## Required later callers

Run these finite bounded synchronous calculations on the existing coordinator
CPU/I/O pool outside short locks and Tokio polling. This module adds no lock,
Tokio task, spawn_blocking, HTTP, storage, heartbeat producer or generalized
binding framework. C03b must consume candidates, compare complete captured base
and owner again, and use the existing one-Match/reconciliation boundary.

Runtime must capture guard generation at admission and finally call
`authorize(generation, exact_observation, fresh_time)` with exact candidate Owner
and original deadline before polling submission or returning an exact retry.
Pure validation grants no live certainty. Authenticated initialized endpoint/
storage/catalog probe checks remain mandatory. C03d must produce and runtime
must finally recheck live generation, sequence, controller boot and anchors so
new advancement invalidates stale expiry evidence even if the root token stays
unchanged. Root association is deliberately conservative; unrelated root progress
requires a freshly associated input without refreshing its time anchors.
Submitted-work registration, cancellation settlement, bounded fresh recovery,
final authority, projected status admission and authenticated reconciliation
remain later work. No aggregate C03/runtime guarantee is claimed.

## Final verification and release

The final eight membership groups passed, 164 filtered, 3.15s after 33.71s
compilation. Final assertions include exact consumed observation/owner, proposal
and revision, value-free Debug, negative Clone guard and retained stop records.
No behavior changed after that passing run.

Additional focused regression commands, each with the environment above:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::verify::tests::reviewed_enrollment_and_claims
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_state initial_roots_and_independent_sequences_roundtrip
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
```

The enrollment regression passed one test, 171 filtered, 0.00s. It checks the
reviewed claims/verifier boundary consumed here. The root regression passed one
test, nine filtered, 0.01s after 16.26s compilation. It checks generation-2 root
round-trip and independent revision/admin sequences without rerunning the codec
rejection matrix. No other existing regression suite was run.

Strict library Clippy first rejected only RegistrationPlan's large enum variant.
The adopted direct owned variants are retained with a documented allowance on
that enum alone. Clippy measured the whole enum at 912 bytes. This preserves the
specified API without adding an indirection; each calculation has one bounded
plan. The second strict library Clippy passed without diagnostics in 10.98s.
Tests were not repeated after this lint-allowance-only edit. No test-target or
all-target Clippy ran; six known unrelated test lints remain parent cleanup.

Final scoped formatting command passed:

```sh
rustfmt --check --edition 2024 --config skip_children=true server/src/cluster/membership.rs server/src/cluster/membership/tests.rs
```

Scoped git diff --check and direct byte checks passed for all three owned files,
including untracked files, with final newlines and no trailing whitespace.
Observed disk headroom moved from 2.4 GiB to 2.3 GiB. There were no dependency/
manifest/lock changes, target deletion, default/AWS builds, fetches, real secret/
.env/profile reads, cloud/network calls, runtime/Python/shared-module edits,
delegates, branches/worktrees, commits or pushes.

Cargo/build and Rust implementation ownership is RELEASED to the parent.
Ready for independent specification then quality reviews. No aggregate C03 or
runtime completion is claimed.
