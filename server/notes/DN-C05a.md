# DN-C05a fixed callback records and retained enrollment

Implements only adopted DN-A19 mini 1. Parent specification review followed by
independent quality review remains required. No aggregate C05 completion claim.
Applied test-driven-development and unslop. No applicable AGENTS.md found in
ancestor directories or owned paths. Worked alone on the shared checkout,
without branches, worktrees, commits, pushes or delegation.

## Ownership and inspected source

Exactly six owned files:

- `src/cluster/callback.rs`, new private module.
- `src/cluster/callback/tests.rs`, new private tests.
- `src/cluster/attachment.rs`, only enrollment accessor and cfg(test) module visibility.
- `src/cluster/attachment/tests.rs`, only fixture visibility and accessor test.
- `src/cluster/mod.rs`, only `mod callback;`, explicitly authorized by parent.
- `notes/DN-C05a.md`, this evidence note.

Pre-edit snapshots of the three existing source files are retained outside the
checkout at `/var/folders/y7/8q3dh_z95ls92c5x7j6ygh500000gn/T/meshdb-c05a-before-cy14oenq`.
Diffs against them show original helpers unchanged except narrow fixture
visibility. Existing unrelated tracked/untracked changes were preserved.
Cargo manifests, lockfile and dependencies were not edited.

Inspected shared contracts, C05/testing rubric, adopted A19 common/mini-1 rules,
A05 lifecycle/authorization, A08/A10, A11/A14, A04 with completed-cache removal,
and current A03 shapes. Inspected actual Python `_callback_protocol.py` and
`_callback_pool.py`, attachment, retained material/composition/files/identity,
Template/Control/member codecs, strict object visitor and bounded writer.
C03a/b notes provide reviewed context only. No unfinished replay implementation
is imported or required. Existing review/test claims in these sources are
reported evidence, not personally rerun checks.

## Exact private API

`AttachedConfig::callback_enrollment(&self, current: &Control)
-> Result<&str, StartupError>` has cluster-parent visibility. It validates the
complete current root and compares cluster/name/config/provisioning/Template/
Timing exactly against the retained root. Policy/controller/member progress is
permitted. It borrows original retained enrollment. Failure is the fixed
`IncompatibleCluster` category, with no underlying error source. No files open,
material bundle, token setter or backend token accessor was added.

`StartCommand::build(root, owner, intent_id, invocation_id, attached)` and
`InventoryCommand::build(root, owner, invocation_id)` return existing
`StorageError` categories. Each command owns immutable bytes, exact-byte SHA256
and opaque ReplyContext, with borrowed `bytes()`/`digest()` and consuming
`into_parts() -> (Vec<u8>, ReplyContext)`. None implements Clone or Deserialize.
Construction validates complete root, managed mode and held owner pair, and
start selects the retained start intent/template fingerprint. A retained
delivery additionally binds invocation/owner/epoch. This is structural local
association, not a time/lease/eligibility authorization grant.

Start uses the ORIGINAL stored Template hints and retained enrollment only in
memory, with null advertisement. It never calls the incoming worker decoder
that requires a filled advertisement URL. Inventory has null intent/target and
an empty payload, with no durable intent allocation. No stop builder or stop
eligibility is supplied. ReplyContext stores complete-root OwnershipObservation,
cluster, owner/epoch, invocation, intent association and expected action.

`validate_start_bytes(raw, root, attached) -> Result<String, StorageError>`
strictly decodes original bytes and separately reconstructs the complete expected
payload/target and identities. It hashes original bytes only after comparison;
noncanonical whitespace is accepted and changes the hash. Invocation is bound
when a retained delivery exists; an unclaimed start can select a fresh invocation.
Hash equality never supplies authority or checks live local guard generation.

`decode_reply(status, raw, &ReplyContext) -> Result<CallbackReply, StorageError>`
returns Started(resource), Stopped, Inventory(InventoryEvidence), or sanitized
Failure(code, DeliveryOutcome). Success is correlated and action typed. Failure
status/code/outcome combinations follow actual Python pre-entry/entered rules,
including inventory's not_invoked failures and callback_in_progress unknown.
Allowed null-correlated preparse failures become Unknown, never NotInvoked.
The fixed wire envelopes use required fields, strict objects, canonical UUIDs,
exact unsigned numbers, duplicate/unknown rejection and required nulls.

InventoryEvidence retains cluster, owner/epoch, invocation, admitted complete-root
observation and complete records. Its borrowed getters expose owner, invocation,
observation and records. Opaque InventoryRecord offers borrowed resource/state
and copied UUID getters. Both have private fields and no Deserialize/unchecked
constructor. A separate private wire DTO is decoded and validated before opaque
records are constructed. All externally usable records have value-free Debug.
Unknown valid resources remain in evidence for later quarantine/reconciliation.
Neither records nor evidence prove provider truth or permit attachment/stopping.

## Memory and validation bounds

Every incoming complete command/reply is capped at 1 MiB before typed allocation,
with the existing depth-32 lexical precheck. Outgoing commands use the existing
bounded writer with a 1 MiB cap. There is no unbounded Value parse in production,
serializer framework, file reopen, external call or secret logging. RawValue
selects a fixed payload/result decoder; the complete envelope remains bounded.
Resource strings are nonempty UTF-8, at most 256 bytes and control free.
Complete inventory is limited to 4096 records; duplicate resources and duplicate
active node OR request bindings reject, while terminated records may share old
bindings. Input DTO allocation is bounded by the complete 1 MiB body; the count
check occurs before opaque evidence construction. Sets are bounded by that input.
Root validation/observations reuse the existing complete 32 MiB bounded encoder;
these synchronous operations belong on the later finite coordinator executor.
This records allocation bounds, not measured peak RSS. Retained material and
S3's currently unsupported composition status are unchanged.

## Personally executed checks and TDD provenance

All Cargo invocations used this exact environment prefix and existing target:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Commands below include `--locked --offline --no-default-features` on every run.
The initial accessor test compiled against an error-returning stub, then failed
its valid-baseline assertion. The resource test compiled against a false-returning
stub, then failed its valid resource assertion. These are the two observed
behavioral reds. The latter had one unused-import warning in the temporary stub.
Remaining assertions were added with implementation and are later green checks,
not claimed as observed behavioral red. An intermediate callback compilation
failed because SHA256's installed output array lacked LowerHex, plus an unused
empty-record binding warning; both were corrected. Compilation failure is not
counted as TDD red.

Personally executed final focused commands, all passing:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::callback -- --nocapture
# 6 passed, 0 failed; 0.36s test execution.
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib callback_enrollment_retains -- --nocapture
# 1 passed, 0 failed; 0.01s.
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib preserves_loaded_noninitial_root_with_relocated_material -- --nocapture
# 1 passed, 0 failed; 0.08s.
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
# Passed, no warnings; initial final check 29.54s.
rustfmt --check --edition 2024 --config skip_children=true server/src/cluster/callback.rs server/src/cluster/callback/tests.rs server/src/cluster/attachment.rs server/src/cluster/attachment/tests.rs server/src/cluster/mod.rs
# Passed.
```

Final Clippy recheck passed in 16.49s after narrowing the fixture write helper
back to its original private visibility, verifying the final source state. The scoped Python
whitespace/newline check reads all six owned files, including untracked files,
and verifies no trailing spaces/tabs and exactly one final newline. It passed.
No full suite, repeated TLS/crypto matrix or AWS/default-feature build ran.

The six callback groups cover literal independent start/inventory exact-byte
hashes, retained material after relocation/deletion with zero observed opens,
valid baseline before independent owner/enrollment/target/payload corruption,
noncanonical whitespace, retained invocation mismatch, start string/stop null,
complete inventory with unknown resources, duplicate resource and active-binding
rejection, another-cluster rejection, terminated-binding parity, actual 4096
success/4097 rejection under the body cap, status/outcome/correlation/type checks,
body/depth rejection and negative Clone/Deserialize trait assertions. Synthetic
enrollment and start bytes are never printed. Generic malformed/depth grammar
coverage is reused from existing validators rather than duplicated; the deep
reply negative also violates its action result shape and is not isolated proof
of the depth precheck. Private constructor visibility is source-inspected.
Fixtures reuse attachment setup and crate::test_storage. No TLS generation or
children were needed; HTTP client construction performs no request.

## Remaining work and evidence limits

Later C05b/c/d own allocation, exact claims, consumed conditional submission,
first-attempt no-effect restrictions, inventory reconciliation and metadata
acceptance. Runtime must obtain fresh private authorization against current
complete root, guard generation and live renewable authority; exact-byte hashes
alone are insufficient. Supervised single-attempt HTTP delivery must retain
ownership through timeout/cancellation and actual settlement, with no hidden
retry. Provider idempotency, authoritative resource/termination truth, unknown
resource quarantine, projected status/admission gates and drain proof remain
required. No callback executed, provider result verified, authority installed,
network/startup performed, persistence/runtime wired, AWS/default build run or
aggregate lifecycle/C05 claim is made. No network/cloud, .env, real secret or
credential-profile inspection was performed. No target cleanup or fetch.

Cargo/build and Rust implementation ownership are explicitly RELEASED at
completion. Parent owns independent specification then quality reviews.
