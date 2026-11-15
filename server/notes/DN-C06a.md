# DN-C06a pure drain metadata and consumed submission

Implements only adopted DN-A21 assignment 1 on shared main. Applied the TDD and
unslop skills. Read the authoritative distributed contracts, C06/F11 and testing
rubric, adopted A21 common rules/assignment 1, A19 identity rules, relevant
A05/A07/A08/A10/A16 and completed membership, codec, ownership and store notes
and implementations. No applicable ancestor or owned-path AGENTS.md was found.
The parent's current completed prerequisite status supersedes A21's historical
pending wording. A22 remains proposed. No unfinished review results or C05c
callback/delivery implementation were inspected as dependencies.

Owned files are exactly:

- `server/src/cluster/drain.rs`
- `server/src/cluster/drain/tests.rs`
- `server/src/cluster/store/drain.rs`
- `server/src/cluster/store/drain/tests.rs`
- `server/notes/DN-C06a.md`

The parent owns the declarations in `cluster/mod.rs` and `cluster/store.rs`.
Comparison against the parent snapshots at
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-c06a-declarations-60ki07tw/{mod.rs,store.rs}`
shows exactly `pub(crate) mod drain` in the former, and `mod drain` plus the
item-specific unused-import-allowed PreparedDrain reexport in the latter.
Their bodies were preserved; these are not implementation-agent modifications.
The actual shared `submit_control` is reused unchanged.

## Private interfaces and behavior

`begin_drain(&Control, Owner, node_id, drain_id, now_ms, proposal)` and
`clear_idle(&Control, Owner, node_id, drain_id, proposal)` return
`Result<DrainPlan, StorageError>`. DrainPlan has direct Existing(DrainBinding)
and Change(DrainCandidate) variants. The private-field binding retains the full
exact Node, including launch/resource/incarnation/session/verifier/drain.
It supplies no authority. Candidate fields are private to drain and children;
its consuming cluster-parent-visible `into_parts` yields the complete captured
OwnershipObservation, exact Owner and bounded expected Control for store/drain.
The types are non-Clone and have no Serde implementation. Debug prints type names
only. Deferred-caller dead-code allowances are item/impl specific.

Every call validates the whole base and held owner/epoch before target checks.
Every change checks a distinct proposal, checked root revision increment and
bounded full-result encoding. New drain IDs reject every retained A19 identity
role and the new proposal ID. Historical proposal UUIDs absent from the fixed
root cannot be checked; caller freshness remains required. No admission or
projected-status gate is applied to retained transitions.

Beginning drainage accepts starting/ready managed or external nodes, including
an unregistered ambiguous managed launch. It changes only phase and the initial
drain record. Existing drainage converges on its retained ID/time/ack even with
a competing supplied ID, reused proposal or exhausted revision. It still checks
the complete base, owner and target, and refuses codec-valid draining without a
drain record. Lost/stopped nodes cannot begin drainage.

Clearing idle requires the exact retained drain ID and draining phase. It clears
only confirmed_idle, retains ack_seq and converges before growth when already
false. Local invalidation must happen immediately without waiting for this write.
No function authenticates idle, allocates a stop, releases credits or proves
termination. Full cloning preserves controller, policy/independent revision,
admin sequence/head, config/Template/Timing, scheduler, intents/attempts,
first-stop credits, all bindings and unrelated members.

`PreparedDrain::prepare(LoadedControl, DrainCandidate)` consumes both values,
compares the complete captured observation, rechecks exact held owner/epoch and
unchanged controller tuple, and bounded-encodes the candidate. Private fields
retain the original loaded root/token and exact bytes. Consuming
`submit(self, &Storage<B>, original_absolute_deadline)` delegates directly to the
reviewed shared helper. It adds no resend, deadline, reconciliation or algorithm.
Timely nonempty-token acknowledgement, whole-root ObservedExact, definite
Conflict and conservative Unknown retain that helper's semantics. Metadata
outcomes do not install guard certainty.

## Personally executed evidence

Every Cargo invocation used the existing target, this environment prefix and
these shared arguments:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
--manifest-path server/Cargo.toml --locked --offline --no-default-features
```

Commands, with that prefix and shared arguments inserted after `cargo test` or
`cargo clippy`, in execution order:

1. `cargo test ... --lib cluster::drain::tests -- --nocapture`: behavioral red,
   exit 101, all three initial groups failed against rejection-only pure stubs.
2. `cargo test ... --lib drain::tests -- --nocapture`: exit 101, one pure group
   passed and six failed. All four store groups failed against rejection-only
   preparation. Two pure failures were fixture mistakes: confirmed idle needs a
   registered incarnation, and an old proposal absent from the current root is
   not a retained identity. These were corrected without changing production
   rules.
3. `cargo test ... --lib drain::tests -- --nocapture`: exit 0, all seven initial
   groups passed, after implementing preparation and correcting those fixtures.
4. Two additional boundary groups were added after green, with no claim of
   behavioral-red provenance. The first expanded command with the same filter
   exited 101 at compilation because assert_eq required Debug on the opaque
   observation. Changing the test to boolean equality fixed that test-only issue.
5. The expanded command then ran nine groups: eight passed; the external fixture
   failed validation because it did not use the required external:<node UUID>
   resource identity. Corrected only that fixture.
6. `cargo test ... --lib cluster::drain::tests::external_and_unrelated -- --nocapture`:
   exit 0, one corrected boundary group passed. The other eight groups had
   passed in step 5; no production change followed that run.

Tests use real generation-2 Control/Node/Intent codecs, real ready_candidate and
registration_plan, synthetic Ed25519 enrollment through verify_enrollment, and
crate::test_storage::Fake. Pure coverage includes retained phases, exact retries
at MAX, changed-ID convergence, clear preserving ack, changing overflow, owner,
phase, missing drain and identity controls, unrelated state/credits and external
membership. Type checks cover candidate/binding non-Clone and non-Serialize.
Store groups causally gate stale drain versus competing drain/takeover, late
registration versus drain, and applied reply loss versus a later CAS. They
assert captured token, bounded read/write counts, complete roots and no resend.
An expired original deadline causes no write. Gate waits and joins have finite
watchdogs, every gate is released and every spawned task settled/joined before
outcome assertions, including missed observations. No detached provider effect,
sleep-based causality or test scratch is used. The reviewed C03b CAS matrix is
not duplicated.

Final regression, lint and formatting results are recorded below.

## Limits and handoff

This is component-level pure metadata and Fake-storage evidence. It supplies no
aggregate C06 or distributed guarantee. Target selection/C07/G02, live guard and
fresh authority, supervised cancellation-safe submission, authenticated C06b
probes/idle proof, stop allocation/delivery, termination and runtime remain
separate assignments. R04b provides no runtime or idle authority here.

No delegation, branch/worktree, commit/push, dependency change/download, network,
cloud/paid call, secret/profile/.env access, target cleanup, public API/schema,
controller/runtime/HTTP/CLI/Python edit occurred. Shared modified/untracked work
was preserved. Parent assigns a new specification review, then quality review.

The exact distinct Cargo commands executed, including the required prefix, were:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::drain::tests -- --nocapture
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib drain::tests -- --nocapture
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::drain::tests::external_and_unrelated -- --nocapture
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib membership::tests
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::store::tests
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

Existing `membership::tests` passed all 15 groups, exit 0, including retained
late registration, readiness, loss and saturated admission behavior. Existing
`cluster::store::tests` passed all 14 groups, exit 0, covering the unchanged
shared helper and guard outcome handling. These are narrow affected regressions,
not a full suite or new runtime acceptance.

Strict Clippy with `--lib --tests -- -D warnings` exited 0 in 19.25 seconds.
Owned rustfmt used `--edition 2024 --config skip_children=true`, then the same
command with `--check` for the four owned Rust files and both parent declaration
files; both exited 0. Scoped `git diff --check` exited 0. An explicit byte check
also checked every owned file, including untracked content, and both parent
files for trailing whitespace and a final newline. Snapshot comparison verified
the exact parent-only additions described above. A temporary read-copy cleanup
initially used pathlib's unavailable missing_ok argument; it was corrected and
the two owned temporary read copies were removed. Tests create no scratch files.

All Cargo sessions reached actual process exit before another command started.
Rust implementation and Cargo/build ownership are RELEASED. No owned process,
task, provider effect or gate remains running or unsettled. New specification
review and then quality review belong to the parent.
