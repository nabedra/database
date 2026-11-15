# DN-C06c2 proof-bound reserved first-stop allocation

Implemented only the second bounded slice of adopted DN-A21 assignment 3.
C06b, including its delayed-negative correction, and C06c1 are reviewed
prerequisites that PASSED both independent reviews according to the parent's
assignment. Historical pending statements in their notes are not current status.
Parent assigns a NEW specification review, then a NEW full quality review.
Aggregate C06 remains incomplete.

## Ownership and preservation

Exactly five owned paths:

- New server/src/cluster/drain/stop.rs.
- New server/src/cluster/drain/stop/tests.rs.
- server/src/cluster/drain.rs, private child declaration and crate-private reexport
  only, with a temporary unused-import allowance for the C06c3 consumer.
- server/src/cluster/drain/proof.rs, exactly two read-only sibling-visible getters
  on ConfirmedDrain, node_id() and captured_generation().
- This new server/notes/DN-C06c2.md.

Exact original copies of both existing files, with repository-relative paths,
are outside the repo at
/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/dn-c06c2-originals-fxknilcq/.
new-paths.txt records the three paths without predecessors. comparisons.txt and
changes.diff beside the originals record byte-preservation comparisons, hashes
and exact changes. Removing precisely the two added getters reconstructs the
original proof.rs byte for byte. The original drain.rs is an exact prefix;
only the declaration/reexport suffix is added. All original bodies and test
module declarations are preserved. No existing test file was edited.

No applicable AGENTS.md was found in repository/ancestor paths. Applied TDD,
entropy-reducer and unslop. Read the orchestration/testing policy, distributed
contracts, A21 common rules/assignment 3, A08/A10/A19 constraints and the C06a,
C06b, C06c1 and C03d notes and necessary auth/heartbeat/guard/store APIs.
The implementation reuses existing concrete helpers and proof continuation.

Shared main and its extensive modified/untracked work were preserved. There
was no delegation, branch, worktree, commit, push, network/cloud/paid operation,
secret/.env/profile access, dependency/Cargo/lock edit or download, new target
or target cleanup. Cargo used the existing server/target for ordinary build
artifacts. No callback/delivery/reconcile/store/controller/heartbeat/member/
state/auth/runtime file, contract, design, rubric or other note was edited.

## Concrete private APIs

allocate_stop(&Control, Owner, ConfirmedDrain, stop_id, now_ms, proposal)
returns Result<StopPlan, StorageError>. The proof is consumed. Before proposal
work, the allocator calls the reviewed check_for_stop with the original
captured_generation and exact current root/Owner. It gets the exact target
node from the proof. Getters expose existing associations only; they establish
no live authority and do not change proof fields, constructors or algorithms.

The allocator requires managed ownership, retained original launch/start,
concrete resource/incarnation, registered session and complete validated root.
The existing proof check owns exact node/root/drain/ack/time verification;
these checks are not reimplemented or loosened. Unknown resource and external
targets refuse. Unregistered/null-incarnation targets cannot produce the earlier
round/idle proof. A04 nullable callback compatibility is not allocation permission.

StopPlan::Existing owns ExistingStop, whose private fields retain both the
consumed ConfirmedDrain and exact retained stop UUID. Its consuming into_parts
returns those together for later C06c3 use. The proof retains the complete root
that binds that identity. Every retained stop is scanned before convergence;
contradictory associations or more than one matching identity refuse. Terminal
history never triggers a replacement. Exact convergence precedes proposal,
fresh-ID, deadline and revision growth, including revision MAX. No credit is
restored and no history is pruned.

StopPlan::Change contains the actual DrainCandidate and existing
PendingStopAllocation returned by proof.prepare_stop_successor. New stops use
fresh request UUIDs checked against retained identity roles and the proposal,
original launch/node/resource/incarnation, Stop/Pending, zero attempts, null
delivery/error/outcome/template fingerprint, created_at/retry_at=now_ms and
checked deadline=now_ms+drain_ms. Retained records are sorted. Existing change
and base helpers check the held owner, fresh change UUID, checked revision and
complete bounded encoding.

Control::capacity() validates both roots. Checked arithmetic enforces N'=N,
I'=I+1, C'=C-1, unchanged I+C, N+I+C and admission_bytes. This spends the
reserved A08 first-stop credit. It does not call check_new_admission or a
projected-status/new-identity admission gate. Further stop identities remain
outside this API and require ordinary admission.

The actual candidate is consumed by existing PreparedDrain and confirmed with
the actual OwnershipOutcome through the existing pending proof API. Only whole
expected-root Acknowledged/ObservedExact can release successor evidence.
Conflict/Unknown release none. No new writer, generic successor API, local
certainty update, proof constructor, Clone/Serde or test bypass exists.
Debug for both new owners prints only the type name. Errors use existing
sanitized categories. Temporary dead-code allowances are scoped to the concrete
C06c3 function/type/impl consumers; large_enum_variant applies only to StopPlan.

## Tests and causal limits

The local fixture borrows reviewed setup patterns, not behavioral matrices.
It uses generation-2 encode/decode, actual worker signing and reply verification,
RequestContext/bind_reply, heartbeat writer/load/Observations receipt creation,
OwnershipGuard acquisition and existing PreparedDrain/Fake submission. No proof
field, raw receipt setter, duplicate module include or public facade is used.

Twelve groups cover:

- Signed positive drain handoff, progressing actual heartbeat reads, idle CAS,
  consumed check_live, allocator, stop CAS, pending confirmation, exact successor
  check_live and fresh actual guard authorization.
- An independently codec-valid 1365-node/1365-start root with 1365 reserved
  credits. The base passes admission. A structurally valid additional ordinary
  node/start candidate fails admission. First-stop conversion encodes, submits
  and reloads, preserving all capacity equalities, sorting and every unrelated
  root/controller/policy/admin/config/Template/Timing/scheduler field. This
  fixture explicitly configures 60-second expiry/lease and bounded I/O budgets.
  Failed/not_invoked and reconciling acceptance-shaped accounting controls keep
  credit spent and physical nodes retained. These are codec-valid shapes, not
  executed callbacks or provider/termination evidence.
- Two independently authenticated proofs against the same complete root and
  two real CAS attempts against the same loaded token. Both futures settle via
  join!, exactly one pending confirmation succeeds, and a fresh authenticated
  round against the loaded winner converges on its one retained stop.
- Pending and failed retained stop convergence at revision MAX despite colliding
  supplied IDs, repeated proposal and overflowing supplied deadline. Consuming
  Existing extraction preserves a usable proof. Multiple matching retained stops
  refuse rather than choose one. Changed revision/deadline overflow returns
  LimitExceeded; retained-ID/proposal collisions return Conflict.
- External target and registered unknown-resource proofs reach allocator refusal.
  The unregistered/null-incarnation/unknown-resource fixture is codec-valid but
  DrainRound::new refuses earlier, before a proof can exist. A contradictory
  retained incarnation cannot encode a valid root and is tested at that earlier
  boundary. Current binding/root and owner substitution refuse the real consumed
  proof. No allocator branch is claimed for an unconstructible proof.
- Actual applied allocation with lost reply, gated before reconciliation read.
  Exact read yields ObservedExact and proof. An actual takeover CAS while the
  gate is held yields Unknown for the old allocation and no proof, preserving
  its retained stop. The injected lost-response flag is disabled for takeover.
  Bounded gate observation records results, releases the gate, settles the
  original task or aborts and joins it, removes the gate and reloads before
  assertions. No task is detached and no sleep supplies causal evidence.
- Wrong expected root and explicit Unknown/Conflict even with an actual loaded
  expected root cannot confirm. Real competition and gated takeover separately
  supply causal Conflict/Unknown controls. New owners have negative Clone/Serde
  compile-time checks and value-free Debug assertions.

No callback entry/execution, claim-CAS chain, positive termination, retry/takeover
inventory flow, process/provider probe or external service is exercised.

## Personally executed commands and provenance

EVERY Cargo invocation used:

CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0

Every test command had this exact form:

cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib FILTER -- --test-threads=2

Sequential runs, each awaited to actual exit before source edits, format or the
next Cargo command:

1. cluster::drain::stop, /tmp/dn-c06c2-red.log, exit 101: compile-only fixture
   delimiter error. No behavioral evidence.
2. Same filter, /tmp/dn-c06c2-red2.log, exit 101: compile-only fixture attribute
   and import errors. No behavioral evidence.
3. Same filter, /tmp/dn-c06c2-red3.log, exit 101: compiling behavioral red, 0/1
   passed. Real signed/heartbeat/idle-CAS setup and check_live reached the closed
   allocator stub, whose Conflict caused the allocation-success assertion to
   fail. Build 40.56s, tests 0.04s. This preceded production allocation behavior.
4. Same filter, /tmp/dn-c06c2-green1.log, exit 0: 1 passed, build 36.95s, tests
   0.06s. A temporary unused import warning remained for subsequent tests.
5. Same filter, /tmp/dn-c06c2-expanded.log, exit 101: compile-only fixture
   immutable-variable error after setup generalization.
6. Same filter, /tmp/dn-c06c2-expanded2.log, exit 101: 9 passed, two fixture
   failures. Saturation exhausted its old ten-second absolute storage budget;
   takeover retained the lost-response injection and was ObservedExact instead
   of the expected Acknowledged. Build 41.27s, tests 11.21s.
7. Same filter, /tmp/dn-c06c2-expanded3.log, exit 101: 11 passed, saturation
   heartbeat publish still used an old pre-validation I/O deadline. Build 48.21s,
   tests 6.62s. Its heartbeat I/O budget was then anchored at heartbeat work.
8. Same filter, /tmp/dn-c06c2-stop-green.log, exit 0: all 12 passed, no warnings,
   build 39.41s, tests 12.99s. Additional groups have green-only provenance;
   fixture failures do not establish missing production behavior.
9. cluster::drain, /tmp/dn-c06c2-drain.log, exit 0: 38 passed, build 1.49s,
   tests 12.86s. This checks the affected proof getters, original drain bodies
   and new allocation together, including the delayed-negative regression.
10. cluster::store::drain, /tmp/dn-c06c2-store.log, exit 0: 4 passed, build
    0.91s, tests 0.05s. Existing submission/CAS/enrollment regressions reused.

With the same required environment:

cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings

Passed, exit 0, 25.69s, no diagnostics. /tmp/dn-c06c2-clippy.log.
These are separate runs, not a combined final suite. No Rust source changed
after these final passing runs. No broad/default/S3/runtime suite was run.

Formatting was restricted to owned Rust with skip_children=true:

rustfmt --edition 2024 --config skip_children=true server/src/cluster/drain/stop.rs server/src/cluster/drain/stop/tests.rs server/src/cluster/drain.rs server/src/cluster/drain/proof.rs

Later fixture-only formatting used the same options on stop/tests.rs alone.
Final rustfmt --check uses those same four paths/options. Direct byte checks
include all five owned paths, including new/untracked files, require exactly one
final newline and no trailing whitespace. Scoped git diff --check and the
snapshot comparisons also pass. Git's tracked diff alone cannot cover these
untracked files, so direct checks and original-copy comparisons are required.

## C06c3 obligations and release

Callers must consume check_live with actual current Observations/OwnershipGuard
and perform final fresh authorization using the original captured generation,
exact complete observation and paired time. A metadata acknowledgement, getter
or allocator result cannot supply fresh authority. Any unrelated newer root or
takeover needs a new authenticated round. The proof and original submission
budget remain owned through confirmation/cancellation.

C06c3 retains stop claim, immutable callback preparation and claim-CAS proof
continuation, fresh inventory retry/takeover, complete/uncertain_stop and later
authorization-chain extension. C06d retains positive termination. Controller,
heartbeat/runtime loops and execution supervision remain later integration.
Command/proof metadata establishes no provider truth, callback entry,
termination or runtime/aggregate C06 guarantee.

Rust implementation and Cargo/build ownership are explicitly RELEASED to the
parent. All initiated processes exited, including every Cargo/formatter and the
owned gated submission tasks. No gate or detached task remains. NEW independent
specification review, then NEW full quality review, is required.
