# DN-C06c1 bounded stop records and shared delivery preparation

Implemented only the first bounded slice of adopted A21 assignment 3. Parent must
assign NEW specification review, then NEW quality review. No aggregate C06,
runtime, provider, idle or termination guarantee is established.

## Ownership and preserved baseline

Exclusive owned files:

- server/src/cluster/callback.rs
- server/src/cluster/callback/tests.rs
- server/src/cluster/provision/delivery.rs
- server/src/cluster/provision/delivery/tests.rs
- server/notes/DN-C06c1.md, new

Exact pre-edit copies of the four Rust files, retaining repository-relative paths,
are outside the repo at
/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/dn-c06c1-originals-0af0v9ws/.
The new note had no predecessor. comparisons.txt and final unified diffs beside
the snapshots record SHA256 values and preservation checks.

All original delivery test bytes remain an exact prefix. All original callback
test bytes remain an exact prefix after precisely one required adaptation:
inventory_literal_command_hash_and_stop_null_positive replaces its synthetic
context(... ReplyAction::Stop) statement with StopCommand::build against a
codec-valid retained stop root and consuming into_parts. Every original assertion
is unchanged. No other original test body differs.

No applicable AGENTS.md was found in ancestors or the repository. Applied TDD,
entropy-reducer and unslop skills. Inspected rubric orchestration/testing and
C06c1/c2/c3 boundaries, distributed contracts, A21 common rules/assignment 3,
A19/A08/A10, A04 exact callback wire/authorization/errors, C05a/c/d, C06b and actual
callback/delivery/reconcile/proof continuation interfaces. Historical pending
statements do not override the parent's reviewed-dependency status.

Shared tracked and untracked work was preserved. No delegation, branches,
worktrees, commits, pushes, dependency/Cargo changes or downloads, new target or
target cleanup, network/cloud/paid operation, secret/profile/.env reads occurred.
Only existing server/target was used. Fixture-created synthetic material uses the
existing bounded directly owned OpenSSL fixture.

## Private APIs and behavior

callback.rs adds StopCommand::build(&Control, Owner, intent_id, invocation_id),
bytes(), digest(), borrowed context(), and consuming into_parts(). StartCommand
also gains borrowed context() for the closed common preparation. These types have
private fields, no raw constructor, Clone or Serde, and value-free Debug.

expected_stop validates the complete root and exact currently held owner through
existing helpers. It resolves the retained stop, managed node and original start,
preserves original launch request_id and retained nullable incarnation_id, checks
the resource association, null stop template fingerprint and any retained delivery
invocation/owner/epoch, then reconstructs A04's exact envelope. Payload is exactly
{resource_id}. No enrollment or material is accessed and no attachment getter was
added. Null incarnation remains valid for exact never-registered bindings.
C06c2's proof-bound allocator must separately refuse that target.

Serialization uses the existing complete 1 MiB bounded writer once. Immutable
bytes, digest and context stay together. validate_stop_bytes(raw, root) strictly
decodes fixed outer/target/payload objects, reconstructs independent expected
semantics, compares them, then hashes the ORIGINAL incoming bytes. Valid whitespace
or field order variations need not share the encoder's digest. Shared depth/body
bounds and strict nested object decoding are unchanged.

ReplyContext replaces the start-only association with private DeliveryAssociation
and closed DeliveryBinding::Start / Stop(resource). Only validated serialization
populates it. matches_delivery checks complete current root validity, cluster,
exact intent/action, node/original launch, invocation, held owner/epoch and immutable
digest against current in_progress delivery. Stop additionally checks exact
node/intent resource and incarnation. Start keeps its reviewed null wire target
without requiring the current node to remain unregistered or resource-free.
Inventory has no delivery association. Original observation remains provenance;
whole-root claim confirmation remains separate.

delivery.rs adds provision-private ClaimCommand::{Start, Stop}, PreparedClaim,
PendingClaim and ConfirmedClaim, all private-field, non-Clone/non-Serde and with
value-free Debug. StartClaim/PendingStart/ConfirmedStart preserve their consuming
external signatures and use these actual shared owners.

prepare_claim(root, owner, idx, invocation_id, command, original_observation,
proposal) returns PreparedClaim. It derives checked attempts+1/max_attempts,
sets exact delivery tuple/digest, checks the validated command's association
against that complete prospective delivery, then uses existing finish for checked
revision/proposal and bounded encoding. Initial and reconciled eligibility remain
separate and unchanged. Their existing arithmetic preflight remains before
reconstruction, including initial overflow error precedence, using claim_attempts.
Common preparation independently rechecks the increment/limit at its boundary.
Prospective retry reconstruction changes only the old delivery identity tuple
before building, retaining old digest/history until replacement. It never resets
a reconciling intent to pending.

PreparedClaim::into_parts consumes into candidate plus PendingClaim containing the
entire expected root and command. PendingClaim::matches_outcome borrows the actual
OwnershipOutcome; confirm consumes once and accepts only whole-root Acknowledged
or ObservedExact. ConfirmedClaim consumes into bytes/context. Future stop wrappers
must check the same outcome alongside C06b PendingStopClaim before consuming that
outcome for the proof. Unknown/Conflict release no command.

failure_candidate(root, mutable_result, idx, now_ms, definite) is the existing
failure mutator with provision-private visibility. Existing complete_start and
uncertain_start use it. It shares existing capped intent/global delay, checked
failures/timestamps, retained maximum deadlines and sanitized categories. The
start-specific first-attempt/unregistered/unknown-resource/no-retired-boot/no-prior-
ambiguity checks remain at the start caller. No-effect semantics, credits,
counters, history and storage submission behavior are unchanged.

Temporary item-specific allowances name C06c3 for the unused ClaimCommand::Stop
branch and StopCommand::build, and future authorization for StopCommand::bytes and
validate_stop_bytes. Existing start/runtime allowances remain. There is no
module-wide allowance or generic transaction/runtime/client framework.

## Test evidence and red provenance

The independent stop literal is written directly from A04. A local Python script
hashed that literal with hashlib.sha256, independently of production serialization
and sha2. Its fixed expected hash is
398c6ff510c49b18da83f72430c18af24a71997103dd42ccdc6fd312b0f8116a.
The fixture uses actual generation-2 Control encode/decode and retained start/node/
stop bindings. Tests exercise real build/validate and actual stop-null decoding.

New callback coverage includes original-byte digest variation; otherwise-valid
wrong target node/original launch/incarnation/resource/owner/action/intent;
raw duplicate envelope/payload fields and unknown nested fields; no material reads;
nullable and registered incarnation controls; exact successor association and
independently codec-valid changed resource/incarnation/node/launch/digest/invocation/
delivery owner/epoch; wrong caller owner; start/stop/inventory separation; redacted
Debug and negative Clone/Serde boundaries. Existing shared tests retain generic
body/depth/resource bounds and strict result/status/outcome coverage. No duplicate
bounds matrix or manufactured StopClaim/proof was added.

Initial focused stop compilation failed because StopCommand and validation did not
exist. That was compile-only evidence. With real byte construction implemented but
association still start-only, the two focused tests compiled: byte/semantic coverage
passed, while the valid stop claim successor failed its matches_delivery positive
assertion. This is the observed behavioral red. Association implementation then
reached a new negative fixture whose changed launch broke intent ordering; that
InvalidRecord fixture failure is not behavioral-red evidence. Changing that launch
to retain sorted intent IDs fixed the fixture. All other new assertions, including
registered-positive, opacity and extraction coverage, have green-only provenance.

The existing 14 delivery groups plus new negative-trait boundary group passed.
They exercise real initial/reconciled claims, complete expected-root confirmation,
unknown/conflict withholding, failure arithmetic/no-effect controls, late draining
registration/resource success, retained credits/history and causal takeover with
actual Fake submission. Existing task/gate ownership settles before assertions.
All 13 affected reconciliation regression groups passed through the extracted path.
No new task, gate, mock, public test facade, duplicate module include, process or
network probe was introduced.

## Personally executed commands and results

EVERY Cargo command used this environment:

CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0

All tests used:
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib FILTER -- --test-threads=2

Executed filters in order, with actual process exit before subsequent Cargo,
source mutation or formatting:

1. cluster::callback::tests::stop_: compile-only failure, exit 101.
   /tmp/dn-c06c1-red-compile.log
2. cluster::callback::tests::stop_: 1 passed, 1 behavioral failure, exit 101.
   /tmp/dn-c06c1-red-behavior.log
3. cluster::callback::tests: 8 passed, 1 InvalidRecord fixture failure, exit 101.
   /tmp/dn-c06c1-callback.log
4. cluster::callback::tests: 9 passed, exit 0, after fixture/association changes.
   /tmp/dn-c06c1-callback-green.log
5. cluster::provision::delivery::tests: 15 passed, exit 0, after shared extraction
   and association checks. /tmp/dn-c06c1-delivery.log
6. cluster::callback::tests: 10 passed, exit 0, after registered/trait additions
   and stop hash computation consolidation. /tmp/dn-c06c1-callback-final.log
7. cluster::callback::tests: 10 passed, exit 0, after restoring the original
   start-test diagnostic statement. /tmp/dn-c06c1-callback-preserved.log
8. cluster::provision::reconcile::tests: 13 passed, exit 0.
   /tmp/dn-c06c1-reconcile.log

Then, with the same environment:
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings

Clippy passed, exit 0, no diagnostics, 22.74 seconds.
/tmp/dn-c06c1-clippy.log

These are separate runs, not a combined final suite. Delivery assertions passed
before the final callback-only refinements and test-name cleanup; its production
extraction was unchanged afterward. Prior reviewed passes are dependency evidence,
not new execution evidence here. No default/S3, broad library or runtime acceptance
suite was run.

Formatting is scoped to the four owned Rust files:
rustfmt --edition 2024 --config skip_children=true server/src/cluster/callback.rs server/src/cluster/callback/tests.rs server/src/cluster/provision/delivery.rs server/src/cluster/provision/delivery/tests.rs
rustfmt --check --edition 2024 --config skip_children=true server/src/cluster/callback.rs server/src/cluster/callback/tests.rs server/src/cluster/provision/delivery.rs server/src/cluster/provision/delivery/tests.rs

Final direct byte checks include this untracked note and all four Rust files,
requiring no trailing whitespace and exactly one final newline. Scoped
git diff --check covers exactly these five paths; direct checks and snapshot
comparisons cover untracked files that Git's tracked diff omits. Formatting,
direct whitespace/final-newline checks, scoped diff check and both test-preservation
comparisons passed, exit 0.

## Integration obligations and release

C06c2 implements exact proof-bound reserved first-stop allocation, rejecting
unregistered/null-incarnation C06 targets, preserving A08 credits and confirming
the complete successor through C06b. C06c3 implements typed StopClaim, exact
allocation/claim proof continuations, fresh inventory retry/takeover and conservative
outcomes through these common helpers and existing PreparedProvision. Reuse the
reviewed classify_inventory and InventoryClass; scanner/proof/store APIs were not
edited here. Future stop must retain command, proof, submission and outcome ownership
through cancellation and release them together only after exact confirmation.

Command bytes, digest validation and typed preparation grant no fresh authority
or callback permission, prove no idle state or termination, and establish no provider
truth, runtime or aggregate C06 guarantee. Later integration still needs fresh
complete-root/current drain proof checks, actual guard authorization, bounded
coordinator execution and independently progressing renewal/authorization/callback
ownership. Stop success/null is acceptance only.

Rust implementation and Cargo/build ownership are RELEASED. All initiated
processes exited; no owned task, child, gate or Cargo/format command remains running.
