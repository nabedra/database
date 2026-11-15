# DN-R03a1: private canonical authentication records

Implemented adopted A15 mini 1 only. R03a credentials, proofs, replay/admission,
HTTP and membership/lifecycle integration remain open. Independent specification
and quality reviews belong to the parent.

Owned writes: `src/node/auth.rs`, `src/node/auth/wire.rs`,
`src/node/auth/wire/tests.rs`, this note, and only the
`pub(crate) mod auth;` declaration in `src/node/mod.rs`. The existing module
allowances/declarations are preserved. No dependency/lock changes, other Rust
implementation edits, branches, worktrees, commits, pushes, network/cloud calls,
dependency fetches, or real secret/.env reads were made.

Read the shared contracts, R03/C03 and testing rubric, adopted A07 including final
corrections, A15 mini 1 and following caller contracts, A05/A06/A09/A11 context,
and actual job/frame, protocol object visitor, member UUID/endpoint and startup
URL implementations. No applicable AGENTS.md was found. TDD and unslop skills
were applied. This child performed the work without delegation.

## Private API and encoding

`auth::wire` is private. `auth.rs` explicitly re-exports these crate-private types:
`ActualRoute`, `Auth`, `ReplyAuth`, `Sender`, `Receiver`, `Role`, `Scope`, `Route`,
`Registration`, `EnrollmentUnsigned`, `Enrollment`, `EnrollmentReply`,
`Readiness`, `ReadinessReply`, `Dependency`, `DependencyCloseReply`, `Drain`,
`DrainReply`, plus `decode_signature`. All DTO fields are private. There is no
public library facade or integration-test visibility expansion. Private tests
are under wire.

Validated byte constructors:

- `ActualRoute::new(method: &str, path: &str, dependency: Option<&str>)` validates
  the actual caller method/literal path/dependency, separate from Auth claims.
- `Auth::decode(header: &str, actual: &ActualRoute)` and
  `ReplyAuth::decode(header: &str)` return shape-checked records;
  `canonical_bytes()` returns bounded exact canonical JSON for the next signing
  slice. Reply association/status expectations remain that slice's obligation.
- Every small record has `decode(&[u8])` and bounded `encode()`.
  `Enrollment::unsigned_bytes()` reconstructs the separate EnrollmentUnsigned
  bytes. `Dependency` is the identical open request/reply and close request shape;
  `DependencyCloseReply` adds the required closed boolean.
- Auth accessors expose cluster, sequence, scope, complete sender/receiver,
  execution claims and content digests. Sender exposes role and identity fields
  plus checked fixed public-key bytes. Receiver exposes its three UUIDs.
  ReplyAuth exposes cluster, request sequence, scope, status, complete
  sender/receiver and content digest. Enrollment exposes identity fields,
  fixed public-key/proof bytes and reconstructed unsigned bytes.
  Accessors grant no signature, freshness or membership guarantee.

These are byte-oriented constructors; runtime send-side construction and any
additional concrete small-body accessors should be added with their owning
callers. No generic sign-arbitrary-domain API exists. Records have no revealing
Debug implementation. Error is exactly the fixed, source-free categories
`Invalid`, `Limit`, `Replay`, `Exhausted`, with matching sanitized Display/Debug.
Replay/Exhausted are reserved for the next replay slice.

Serialization declaration order is the A07/A15 displayed order, including nested
Sender/Receiver and complete Sender objects in replies. EnrollmentReply is the
separate unsigned trusted-channel response with registered/verified only.
Required nullable fields stay required. Version is 1. String-only enums reject
Serde object enum forms. The reused object visitor plus strict derived Wire
fields reject arrays, unknown fields and duplicates at every record level.
Member UUID adapters enforce lowercase hyphenated spelling. RawValue is borrowed
only for exact integer token inspection: digits, parsed u64, no Value/tag buffer,
marker object, sign, fraction, exponent or overflow. All capacities and positive
sequences are checked. Worker/API identity and the closed six-route claim table
are enforced against the actual caller route and dependency header.

Auth header length is checked at 8192 before lowercase/even hex decoding into at
most 4096 bytes. Signature validates exactly 128 lowercase hex bytes before
producing 64 bytes. Auth and ReplyAuth require exact equality with bounded
canonical re-encoding, including order, escapes and whitespace. Enrollment
accepts otherwise valid noncanonical JSON and reconstructs unsigned proof bytes.
All small bodies cap at 16 KiB. A lexical depth-16 preflight precedes recursive
DTO allocation; the existing configuration scanner has a different depth-32
contract, so it cannot implement this limit directly. The fixed writer reuses
`configuration::bounded`; UUIDs reuse member helpers, and endpoints reuse
`configuration::api_url` (member endpoint validation plus no-prefix URL rule).
No complex parser was duplicated or unowned helper changed.

Item-specific pending-caller dead-code allowances cover these records, decoder
implementations and re-exports. Remove allowances as R03a2 and subsequent
HTTP/enrollment callers land. There is no module/global lint suppression.

## Tests and commands

Every Cargo invocation used exactly this environment prefix, the existing target,
and the common arguments shown below:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::wire::tests
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_configuration constructed_candidates_enforce_paths_capacities_and_encoded_limits
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
```

Tests were written before decoding implementation. The first run with an empty
wire module failed compilation for missing APIs; that is not behavioral red.
With compiling DTO/API scaffolding and a temporarily rejecting decode boundary,
the initial five groups produced four behavioral failures on independently
written valid literals and one pass. After adding worker/nested groups while
still rejecting, seven groups produced five failures/two passes. Removing that
boundary produced seven passing groups. Bounds/negative-only groups that passed
on their first execution have no separate behavioral-red claim. Thin accessors
and their literal-association assertions were added afterward; no additional red
is claimed for those assertions. No fixture expectation was changed to make a
failure pass, and no real signature was generated or verified.

The seven grouped tests cover independent literal Auth/reply/enrollment bytes,
nested order and required nulls, canonical rejection of changed order/whitespace/
escape spelling, enrollment reconstruction, separate enrollment response,
representative malformed nested/unknown/duplicate/type/enum/number-marker cases,
exact u64 limits, route/role/claim/dependency agreement, small booleans and
heartbeat sequence, URL/capacity guards, header/signature hex and byte bounds,
depth 16/17, and the actual bounded writer at cap/cap+1. Allocation safety is
source-level preflight/writer ordering plus bounded fixtures, not measured RSS.
The existing configuration constructed-candidate test checks the reused writer
and URL/capacity guard path; no broader regressions are justified by changes to
this isolated module.

Initial strict library Clippy found a dead-code diagnostic on the pending derived
nullable adapter and recommended as_chunks for hex decoding. Both were corrected.
Final checks/results are recorded below after execution. Broader test-target
Clippy was not run; the parent's six known unrelated diagnostics remain open.

## Remaining obligations

These records prove shape/canonical bytes only. R03a2 must implement ring-backed
key ownership, domain signatures, verifier/HKDF, content checks and pure authority
checks. R03a3 must consume associated checks under bounded replay/final admission.
Fresh Control loading, pinned API key association, status/request association,
transport header multiplicity and framing, supervised pre-admission body hashing,
semantic owners, enrollment transitions and lifecycle behavior remain callers'
work. Jobs keep existing 16 MiB CONTROL/PART_META and 64 KiB DATA bounds; public
operations keep 8 MiB. None of those payloads are collected by this module.
No aggregate R03a, HTTP, crypto or distributed-runtime completion is claimed.

Final execution results: focused auth command passed all seven groups (119
unrelated library tests filtered out); the justified configuration regression
passed one test (six filtered out); strict production-library Clippy passed.
`rustfmt --check --edition 2024 --config skip_children=true` passed for the four
owned Rust files. Direct Python byte checks passed trailing-whitespace and final
newline rules on all five owned files, including untracked files. A direct
comparison against the initial module read confirmed only the authorized auth
declaration was added. The same whitespace checks were rerun after this note's
final update. No broader tests/all-target lint or AWS/default build ran.

Cargo/build and Rust-implementation ownership is RELEASED to the parent. This
mini is ready for the parent's separate reviews; no follow-on slice was started.

## Parent correction: checked construction cannot be bypassed

Independent quality review found that the crate-visible records implemented
serde::Deserialize directly. That allowed callers to bypass byte limits,
canonical equality, actual-route checks and semantic validation. In particular,
a directly deserialized reply with status 65536 reached a truncating status
accessor. Private fields alone did not prevent this construction path.

The parent first added a temporary direct-deserialization regression, with a
valid checked status-200 baseline and the same reply changed only to status
65536. The exact focused test compiled and failed behaviorally with exit 101:
"unchecked deserialization admitted an out-of-range reply status". Compilation
took 24.64 seconds; test execution reported 0.00 seconds.

Unchecked construction now uses a module-private DecodeRecord trait. The local
raw Wire structs retain strict Serde object decoding; explicit private nested
callbacks decode Sender/Receiver fields. Exposed records no longer implement
Deserialize, and their checked entry points still enforce the existing bounds,
canonical bytes and route/content shape. No record field or wire format changed.

The temporary runtime bypass probe was replaced because its direct construction
must no longer compile. The retained grouped regression has compile-time negative
Deserialize assertions for all thirteen exposed records, plus the checked valid
status baseline and rejection of status 65536. A restored Deserialize
implementation makes the marker trait ambiguous at compile time. The assertion
checks the callable trait boundary, not a source-text pattern. No new dependency
or public testing API is used. Existing seven behavioral groups remain intact.

The parent used the same four resource environment settings and locked/offline/
no-default Cargo arguments recorded above. Red command: `test --lib
node::auth::wire::tests::validated_records_require_checked_construction -- --exact`.
After correction, `test --lib node::auth::wire::tests` passed all eight groups
in 0.01 seconds after 23.28 seconds compilation. The first strict library Clippy
found the private nested decoder unused before R03a2 production callers; its
item-only pending-caller allowance now matches the existing decoder allowances.
The repeated `clippy --lib -- -D warnings` passed in 9.00 seconds. Owned rustfmt,
scoped Git whitespace and direct untracked-file whitespace checks passed.
No unrelated suite, broader test lint or real cryptographic check was run.
Fresh independent quality re-check remains pending. Parent Cargo ownership is
released after these checks for the next bounded credential assignment.
