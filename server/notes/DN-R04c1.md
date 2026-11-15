# DN-R04c1 checked authority and initialization seam

Implements only adopted A22 assignment 1 on shared main. C06a released exclusive
Rust/Cargo ownership before work began. No C06a implementation body was inspected.
No delegation, branch/worktree, commit/push, network/cloud/paid/download action,
secret/.env/profile read, dependency/Cargo edit or target cleanup occurred.
Applicable repository/ancestor AGENTS discovery found no instructions. Applied
TDD and unslop. Contract/rubric orchestration, testing, R03/R04 and the adopted
A22 boundaries govern this slice; A20/A21, A18/A17/A07/A15, A06/A09 and reviewed
R04a/b1/b2 including parent readiness teardown, R03a3b/c and C03c/d notes supplied
the prerequisite interfaces and evidence. Earlier pending-review statements are
historical. This note claims no aggregate R04/R03a3 or distributed completion.

## Files and retained snapshot

Pre-edit copies of every existing owned file are outside the repository at
`/tmp/meshdb-r04c1-hsgs3t6s/`, preserving repository-relative paths.

- `server/src/node/admission.rs`: checked DurableRequestView, exact initialization
  transition, private Arc storage adaptation, registered-draining distinction and
  focused private tests. Existing capacity, permit, start, charge and reaping
  transitions remain retained.
- New `server/src/node/initialization.rs`: opaque R07 capability and private tests.
- `server/src/node/mod.rs`: only the private initialization declaration.
- `server/src/node/payload_owner.rs`: only borrowed ProcessReady matching.
- `server/src/node/auth/verify.rs`: AcceptedRequestAuthority declaration, redacted
  Debug and narrow checked predicate reexports.
- `server/src/node/auth/verify/eligibility.rs`: accepted-authority checking and
  shared exact target/sender/API-pin predicates.
- `server/src/node/auth.rs`: narrow reexports.
- Existing private `auth/verify/eligibility/tests.rs`: appended focused tests.
- This new delivery note.

## APIs and boundaries

```rust
DurableRequestView::prepare(
    root: &Control, eligible: EligibleRequest, boot: Arc<ReceiverIdentity>,
) -> Result<DurableRequestView, Refusal>
RequestBinding::check_accepted_authority(
    &self, root: &Control, local: &ReceiverIdentity, pinned_api: &[u8; 32],
) -> Result<AcceptedRequestAuthority, auth::Error>
ReceiverState::complete_initialization(
    &mut self, process: ProcessReady, configured: InitializedWorker,
) -> Result<(), Refusal>
```

The view consumes and retains the original EligibleRequest. Complete bounded
root validation/encoding/hash, observation comparison, exact local commitment,
phase/registration/drain checks and coordinator allocation happen before any
receiver lock. DurableView and StartCheck retain Arc boot identities; production
callers must supply the original LocalAdmission Arc. No incoming Auth reconstructs
that boot. Verified local eligibility proves the local boot coordinator; a worker
request adds only its actual root-checked verified coordinator. There is no list
of caller-declared eligible senders. Registered Starting, verified Ready,
registered Draining, verified Draining and denied facts remain distinct.
Registered drainage observes the permanent barrier but cannot execute work.

The private eligibility predicate recognizes the checked local boot as a
coordinator as well as the bounded incoming checked worker. This supplies local
operation composition without constructing a new Sender record. A fresh view
is scoped to its checked request. c2 must prepare fresh authority for the actual
retained queued subject before start; this view is not a reusable global list
of arbitrary eligible peer keys. No UUID generation or parsing/hash runs in
complete_initialization or the existing locked observation/start methods.
Operation admission/self-permit candidate preparation remains c2's outside-lock
PreparedRoute responsibility.

AcceptedRequestAuthority is auth-issued only from a retained RequestBinding.
Its private original request Arc provides exact invocation association, including
independently verified identical-byte invocations. It retains the fresh complete
observation and checked local boot. Read-only observation(), local() and
matches_binding() expose no proof constructor, Clone, Serde or mutable fields.
The common request_target/current/worker predicates preserve original fresh
request checks, including current commitments, retirement, exact receiver and
API pin. Accepted operation/job work permits verified Draining; fresh operation
and dependency-open predicates remain Ready-only. This authority is provisional.
It performs no signature reparse, replay consume or live start grant.

InitializedWorker has private exact boot Arc/init UUID fields, type-only Debug,
immutable identity matching and cfg(test)-only issuance. No callable production
success constructor exists. R07 must issue it once only after actual configured
backend/catalog validation settles and retain configured resources in its startup
owner until shutdown. It cannot depend on a listener or Ready CAS.
ProcessReady::matches_initialization borrows both opaque identities. The consuming
receiver transition first checks the actual original retained process entry,
success/cancellation and Uninitialized boot/init using check_locked, then exact
configured evidence, and changes only InitState to Initialized. Failure spends
the receipt, preserves process ownership/identity and leaves admission closed.
Readiness, enrollment and durable Ready remain separate.

## Tests and provenance

All Cargo commands used this exact environment and existing `server/target`:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Every command used `--manifest-path server/Cargo.toml --locked --offline
--no-default-features`. Each actual process exit was awaited before another Cargo
command. No default/S3 build or dependency fetch was run.

Observed test-first behavioral reds against closed compiling stubs:

- `accepted_authority_keeps_invocation_and_allows_draining` refused a valid
  retained original binding, exit 101, 28.97s build. Before that behavioral run,
  a mechanical compile error came from assert_eq requiring Debug on the deliberately
  opaque OwnershipObservation. It was corrected to equality assertion; no
  production visibility/Debug change was made.
- `configured_identity_and_original_process_are_both_required` kept the independent
  matching positive Uninitialized, exit 101, 29.04s build. Real process ownership
  was drained, the pool joined and scratch removed before its failing assertion.
- `checked_views_preserve_phase_boot_and_complete_root` refused the valid signed
  readiness proof, exit 101, 31.78s build.

After implementation, `cargo test ... --lib node:: -- --test-threads=2` passed
123 groups, exit 0, 30.21s build and 9.62s tests. This filter was broader than
necessary; its unchanged parser/stream groups were not repeated. It includes
existing admission, eligibility, replay and all payload/ProcessReady regressions.

The later independent coordinator/target group initially failed fixture validation,
not production behavior. The first fixture reused a live incarnation; the next
retired an incarnation still live on the other node. Corrected roots use distinct
current identities and retire each changed node's own original incarnation.
The eligibility filter then passed ten groups, exit 0, 29.97s build and 0.10s
tests. These added negative/trait cases have green-only evidence, not individual
observed behavioral reds. Final strengthening uses distinct sender and target keys.

The signed real-codec view tests retain the original proof and check complete-root
and boot mismatch, Registered Starting, Ready, both Draining registrations,
denied roots, permanent drainage, no registered-job execution and equal-revision
sticky divergence through actual LocalAdmission::observe. Accepted checks consume
real content/proofs through ReplayState and into_handoff before using RequestBinding.
They distinguish identical-wire invocations and accepted drainage versus fresh
operation refusal, foreign local/cluster, current target/coordinator replacement,
retirement, verifier loss and API pin changes, plus unrelated valid root progress.
No raw proof is manufactured and no crypto/parser matrix was copied.

Initialization tests create independent real WorkerPools and start_boot/process_ready
owners for matching success, foreign boot, wrong init, cancellation and legacy
refusal. An unpolled original ready future supplies actual sticky cancellation.
Duplicate readiness cannot issue another receipt. Observations are retained and
original leases dropped, owner drain/reaping awaited, WorkerThreads shutdown/join
awaited and scratch removed before outcome assertions, including readiness failure
or timeout. Every future is bounded by a ten-second watchdog; no detached task,
sleep causality or test fixture that pretends to perform configured startup exists.
Negative Clone/Serialize/Deserialize trait guards protect both new capabilities.

Final focused checks and preservation results are recorded below after execution.

## Remaining owners

c2 owns mandatory shared replay storage, the real accepted registry/request cell,
one-use payload handoff, final locked reservations/consume/install, retained
WorkKey/request/Binding/DependencyKey association, production StartCheck issuance
and next_start composition. Existing opaque StartCheck conditions remain unchanged
apart from boot Arc storage. c3 owns settled heartbeat publication and positive
idle observations. R07 owns the configured issuer and retained startup resources.
HTTP/prelude, fresh GET, scheduler/dispatch, supervision, listener, runtime and real
network integration remain unfinished. No initialization startup success or
positive idle/termination/distributed guarantee is claimed.

Parent must assign a NEW specification review, then an independent quality review.

## Final validation and release

The exact Cargo command forms executed, each with the four environment variables
above, were:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib accepted_authority_keeps -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib configured_identity_and_original -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib checked_views_preserve -- --nocapture
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node:: -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::verify::eligibility -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission::ownership_tests::checked_views_preserve_phase_boot_and_complete_root -- --exact
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::initialization -- --test-threads=1
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

The first command ran once with the mechanical compile error and again for its
behavioral red. Initialization and view stub runs each produced their recorded
behavioral red. The eligibility filter ran four times: nine groups with the first
fixture failure, ten with the second fixture failure, ten passing after fixture
correction, and ten final passing after distinct-key strengthening and shared
predicate consolidation. Its final run exited 0, 29.15s build, 0.10s tests. Final
expanded view ran one group, exit 0, 0.91s build and 0.03s test; initialization
ran two groups, exit 0, 0.85s build and 0.05s tests. Strict library/test Clippy
exited 0 without diagnostics in 19.58s. No passing broad group was repeated.

Owned rustfmt used edition 2024 and `--config skip_children=true` on the eight
owned Rust files. Its final check and direct trailing-whitespace/single-final-LF
check cover all modified/untracked owned files, including this note. Scoped
`git diff --check` passed. Direct comparisons against the external snapshots
confirmed that node/mod.rs adds only its private declaration, payload_owner.rs
adds only ProcessReady borrowed matching, existing auth test text is preserved,
and existing admission assertions/algorithms remain identical after normalization
of the named phase/local-coordinator/Arc/comment deltas and new tests. verify.rs
and auth.rs have only their declared additions; eligibility's existing request
route logic is preserved with its exact common target check extracted. The
comparison script is retained with the external snapshots.

Rust implementation and Cargo/build ownership are RELEASED. All initiated
processes have exited; none remains running. NEW specification review followed
by independent quality review belongs to the parent. c2, c3, R07 configured
startup and all remaining runtime/integration obligations above remain open.

## Parent specification correction: locked coordinator comparison

Independent specification review found that an API-derived view leaves the
incoming coordinator vector empty. LocalAdmission::eligible then called
ReceiverIdentity::matches_sender, whose Sender::public_key accessor decoded hex
through an allocated Vec while the receiver mutex was held.

The parent changed only that production fallback to compare the binding's Sender
directly with the retained LocalAdmission coordinator. The existing incoming
checked-worker vector comparison is unchanged. The now-unused matches_sender
helper was removed from eligibility.rs. Sender key decoding itself is unchanged
in production; wire.rs adds only a cfg(test), thread-local count of decoder calls
and its node-private read accessor. This counter observes parsing attempts, not
all allocations or process RSS, and adds no production identity interface.

The new API-operation regression uses actual signature verification and checked
eligibility/view preparation, confirms an empty incoming coordinator vector,
then reserves local work under the real ReceiverState mutex. A distinct Arc with
equal Sender contents succeeds; the same UUID tuple with a different key is
rejected. A positive decoder control proves the counter is active. The locked
observations are captured before settling the retained local ticket, and the
assertions follow settlement and lock release. It claims local state behavior,
not payload supervisor or configured-startup execution.

Pre-correction copies of admission.rs, wire.rs, eligibility.rs and this note
retain repository-relative paths under
`/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-r04c1-key-compare-8utg30y8/`.
Direct comparison confirms the stated production delta, new test/counter and
operation extension to the existing API-view fixture. Existing test bodies and
other checked-authority algorithms remain unchanged. This establishes current
snapshot relationships, not historical execution provenance.

Every Cargo command below used the original four required environment variables,
existing target, locked/offline/no-default flags and serial process completion:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission::ownership_tests::api_view_local_reservation_compares_retained_identity_without_key_decoding -- --exact
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission::ownership_tests -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::verify::eligibility::tests -- --test-threads=2
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

The first attempt failed compilation because assert_eq required Debug on the
deliberately non-Debug Route; replacing it with an equality assertion changed no
production type. The corrected focused test then failed behaviorally: two locked
key decodes versus zero, after successful ticket settlement (30.90s build, 0.01s
test, exit 101). After the production correction, all twelve admission groups
passed (30.43s build, 0.55s tests), followed by ten eligibility groups (0.92s build,
0.09s tests). Strict library/test Clippy passed in 24.17s. Owned rustfmt, direct
whitespace/single-newline checks and scoped git diff --check passed. Earlier
initialization and broad 123-group evidence is reused, not a combined final rerun.

The parent releases Rust/Cargo with no process running. A NEW specification
re-check must close this finding before independent quality review. c2, c3, R07
and all HTTP/runtime/distributed obligations above remain open.
