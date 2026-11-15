# DN-C04c2 pure administration candidates

Implemented only A13 assignment 2 after C04c1's recorded specification and quality
reviews. Owned files are `src/cluster/admin.rs`, `src/cluster/admin/tests.rs`,
this note, and one crate-private `admin` declaration in `src/cluster/mod.rs`.
Shared uncommitted work is preserved. No child delegation occurred.

## API and caller obligations

`pub(crate) fn admin_candidate(root: &Control, request_id: Uuid,
exact_request: &[u8], proposal_id: Uuid) ->
Result<(Control, ControlDecision), StorageError>` is pure. Its caller must
first authorize and strictly admit the original envelope, then establish complete
reachable-history absence through C04c1. Replay must precede this function and
all counter arithmetic, including for exhausted roots. Runtime must supply a
globally fresh proposal UUID. Preparation checks that it differs from the base
change UUID and current administration head, then uses it for both new IDs.
The item-only dead-code allowance on this function is temporary until C04c3
consumes it. No module-wide allowance or public facade was added.

The parser reuses `configuration::input` for the original 1 MiB and depth limits,
`protocol::deserialize_object` for object-only parsing, and `member::uuid` for
canonical UUIDs. Strict serde wire structs reject duplicate, unknown, missing
and mistyped fields. RawValue preserves the payload's original spelling until
strict payload parsing. No Value map normalizes production input. Both bodies
remain exact strings in the decision. Malformed envelopes return sanitized
InvalidCatalog, and oversized input returns LimitExceeded before allocation.
Only well-typed out-of-bounds requests become validation_error/400 decisions.

Fixed scale sets desired/min/max together in 0..1024 and disables auto. Auto
replaces valid 1..1024 bounds and clamps the current desired count. Comparing
the resulting policy with the old policy while revision is unchanged compares
all four effective fields. Only a change increments policy revision. External
fixed goals persist, while valid external autoscale returns the static
unsupported_capability/400 response without changing policy. Invalid bounds take
precedence over capability. Errors use static sanitized messages and
`outcome: "not_applied"`; successes contain the exact correlated Policy. Responses
contain only fixed-width scalars/static text and additionally check the 1 MiB cap.

Each publishable result, including identical policy and deterministic error,
checked-increments root revision/admin sequence and records the cluster, parent,
base/published revisions, exact request and complete response. Exhaustion uses
existing sanitized LimitExceeded without mutation or a fabricated terminal
outcome. Base and candidate both pass bounded Control::encode; the entire
generated decision passes ControlDecision::encode. Preparation clones the full
root and changes only policy, root revision/change UUID and admin sequence/head.
It never calls check_new_admission, projects status or acquires controller
ownership. Generation-1 configuration is cloned opaquely until C08c's coordinated
amendment. No paths, schemas or fingerprints change.

## Tests and executed checks

Eight private pure test groups were written before implementation. A temporary
Unavailable-returning stub compiled, then all eight groups failed behaviorally
with the expected missing-success or wrong-error assertions. This was not a
compile-only red. After implementation, the first build failed on an omitted
ConfigurationError::UnsupportedStorage match arm. The exhaustive mapping was
corrected, then all eight groups passed in 1.86 seconds. No test fixture or
production module outside ownership was changed.

Groups cover fixed/auto transitions, lower/upper clamping, zero and 1024;
identical fixed/auto policy at maximal policy revision and independently
advancing root/admin counters; mode/bounds changes with unchanged desired;
external fixed/auto; deterministic invalid-bound errors before capability;
exact whitespace and escaped operation/field retention at exactly 1 MiB;
representative original duplicate/unknown/missing/type, correlation, canonical
identity and depth rejection; local proposal freshness, invalid constructed base
and each independently exhausted counter; and a codec-valid saturated retained
root without admission room. The later UUID test refinement first establishes
a valid correlated alphabetic UUID baseline, then changes only its letter case.

Successful preparations round-trip through actual Control and ControlDecision
encode/decode, check exact request/response correlation, compare the entire root
against its narrowly permitted changes and compare derived capacity. The
nonempty base retains two managed launches, a first stop and an unused credit,
held owner/epoch/lease, scheduler fields and nonempty history metadata. The
saturated fixture retains 1024 managed launches, 1026 stops and 1023 credits;
new admission fails while policy preparation succeeds with identical capacity.
These are pure fixtures and imply no storage or historical-provider evidence.

All Cargo commands used the existing target, sequentially, offline with the
assigned environment. The focused command ran for the stub red, implementation
checks, and final UUID-test refinement:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::admin::tests
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
```

Strict production-library Clippy passed, exit 0 in 0.88 seconds. An earlier
Clippy invocation mistakenly added `--tests`, which was broader than necessary.
It exited 101 on six existing unowned diagnostics: duplicate Fake module
inclusion, collapsible_if in payload_owner, await_holding_lock in payload_owner
tests and local_tests, replace_box and assertions_on_constants in send_tests.
No suppression or unrelated fix was added, and no test-target Clippy pass is
claimed. No broad test suite or AWS check ran.

Owned format and whitespace checks:

```sh
rustfmt --edition 2024 --config skip_children=true --check server/src/cluster/admin.rs server/src/cluster/admin/tests.rs server/src/cluster/mod.rs
git diff --check -- server/src/cluster/mod.rs server/src/cluster/admin.rs server/src/cluster/admin/tests.rs server/notes/DN-C04c2.md
```

A direct byte check also covers all four untracked files for final newline,
carriage returns and trailing spaces. No runtime wiring, I/O, backend, callback,
publication, replay orchestration, authority or distributed execution is claimed.
No dependency change/fetch, target deletion, branch/worktree, commit/push,
.env/real secret read or cloud/network call occurred. Independent specification
then quality review remain parent-owned. No implementation blocker remains.

Final focused run after the isolated UUID refinement: 8 passed, 0 failed in
2.01 seconds. Owned rustfmt and whitespace checks passed. Cargo/build ownership
is explicitly released on delivery.

Independent specification and quality reviews passed without actionable findings.
The quality review notes one evidence limit: the nested-array negative fixture
also violates the required payload shape, so it does not independently isolate
the depth guard. The guard itself was inspected in source. The broader test-target
Clippy diagnostics above remain unresolved; no passing result for that invocation
is claimed.
