# DN-R03a2b1 signature possession and signed requests

Implemented only adopted DN-A17 mini 1. Independent specification review followed
by quality review remains parent-owned. Minis 2/3, replay, final admission and
runtime integration remain incomplete.

Read the shared contracts, R03/testing rubric, adopted A17/A15, A07 final
corrections, R03a1 including its checked-construction correction, R03a2a delivery
and the actual wire/credential implementations and pinned test vectors. Applied
test-driven-development and unslop skills. No applicable AGENTS.md was found in
ancestors or the repository. No delegation occurred.

## Changed files and APIs

Exactly five files belong to this change:

- New `server/src/node/auth/verify.rs` defines synchronous crate-private
  `verify_enrollment(&[u8]) -> Result<EnrollmentProof, auth::Error>` and
  `verify_request(&str, &str, &ActualRoute) -> Result<SignedRequest, auth::Error>`.
- New private `server/src/node/auth/verify/tests.rs` holds eight test groups.
- `server/src/node/auth.rs` adds the private module and concrete crate-private
  proof/function reexports. Already-consumed wire and verifier imports are
  separated from pending imports.
- `server/src/node/auth/wire.rs` adds only Enrollment's read-only
  `endpoint() -> &str`, `operation_slots() -> u64`, `partition_slots() -> u64`,
  `queue_size() -> u64`, and removes consumed allowances. The consumed sender
  getter is moved into an impl without a pending allowance. Existing decoder,
  validator and getter bodies are preserved, as are private DecodeRecord and
  all existing wire tests. Mixed pending accessor/macro allowances remain.
- New `server/notes/DN-R03a2b1.md` records this delivery.

Enrollment strictly decodes, reconstructs unsigned bytes, verifies the claimed
Ed25519 key against exactly `b"meshdb.enroll.v1\0" || unsigned`, then derives
worker_verifier over checked cluster/node/incarnation/session/key. EnrollmentProof
privately owns the complete Enrollment and fixed public-key/verifier arrays.
Only borrowed `claims()` and copied `[u8;32]` `public_key()`/`verifier()` are
exposed. C03 may hex-encode the verifier into existing Session.verifier.

Requests call existing Auth::decode with the actual route/dependency and
existing decode_signature. Verification uses the claimed sender key and exactly
`b"meshdb.http-request.v1\0" || canonical`. Each success allocates a distinct
private Arc<RequestAssociation> containing the complete Auth and canonical
Box<[u8]>, even when identical bytes are verified twice. SignedRequest owns only
that Arc and exposes only `auth() -> &Auth`.

Both proofs have private fields, no Clone/Serialize/Deserialize, no unchecked
constructor, mutator or detached-parts API, and value-free Debug. Crypto errors
map to static Invalid; existing checked parsers discard their diagnostics.
The private helper accepts a closed enrollment/request domain enum, never
arbitrary domain text. It enforces 16 KiB/4096-byte caps before allocating
prefix+message. Decoders also bound original enrollment/header input before
parsing/hex allocation. No response proof or speculative mini-2 API was added.
Pending caller allowances are item-specific.

## Evidence and chronology

Tests preceded verification implementation. Two positive tests ran against
compiling Err(Invalid) placeholders and both failed behaviorally at unwrap of
reviewed valid vectors, exit 101. Compilation took 26.34s; tests took 0.00s.
This was observed behavioral red, with no missing-API compile-red claim. Three
unused synthetic-fixture warnings occurred in this initial two-test run.

After adding the remaining assertions and implementation, the first green
attempt failed compilation because the bound test reused the moved private
Domain enum. Giving that closed enum Copy/Clone corrected the test-support
issue; opaque proofs remain non-Clone. The next focused run passed all eight
groups, 137 filtered, 0.01s after 23.09s compilation, without warnings. No separate
behavioral-red claim is made for later assertions or negatives.

Literal unsigned enrollment, API readiness request, worker dependency-open
request, signatures and verifier expectation are copied unchanged from reviewed
R03a2a vectors. Their independent Python cryptography/hashlib provenance remains
that delivery's reported evidence; no generator or crypto derivation matrix was
recreated here. Changed claims and wrong domains use synthetic ring Ed25519
signatures directly over test text, without production signing/encoding helpers.

The eight groups cover valid enrollment and getters/verifier, valid API/worker
requests and distinct Arc identity, altered signatures, wrong domain and missing/
double NUL, changed syntactically valid canonical bytes with old signatures and
new-signature positive controls, reordered Auth with both otherwise-valid
canonical and reordered-byte signatures, actual route mismatch, dependency
mismatch with a valid signed job baseline, enrollment verifier changes for each
UUID and key, cap/cap+1 at input and closed-helper boundaries, value-free Debug,
and negative proof Clone/Serialize/Deserialize guards. Existing wire construction
regression is reused unchanged. No benchmark, entropy, RSS or runtime claim.

Every Cargo command used the existing server/target and this environment:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Exact commands, each with that prefix:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::verify::tests
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::wire::tests
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::credentials::tests
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
```

The verify command ran three times as described above. Wire regressions ran once:
eight passed, 137 filtered, 0.01s. They protect the consumed checked decoding,
canonical reconstruction and unchanged no-Deserialize boundary. Credentials ran
once: four passed, 141 filtered, 0.01s, protecting reused key/verifier/domain
expectations. No broader passing suite or test-target/all-target Clippy was run;
six known unrelated test-lint findings remain parent cleanup.

## Caller obligations and retained boundaries

Enrollment possession grants no bearer authentication, initialized-storage
proof, launch/template/capacity eligibility, controller ownership, CAS commit,
readiness or permission to execute. API/C03 callers retain those duties.

SignedRequest proves syntax and signature only. Minis 2/3 must bind exact body/
CONTROL content, replies and retained contexts, then check provisional membership
against exact root observations. Later callers must supply actual HTTP facts,
fresh durable reads, trusted API pins, monotonic root ordering, dependency/local
checks, sequence/replay and atomic capacity reservation before execution. No
body hashing/polling, durable read, transport claim or permission cache occurs
here. Existing payload, TLS, EOF, supervisor and database outcome duties remain.

Shared uncommitted/untracked work was preserved. No manifest/lock, credentials,
wire-test, public API, cluster, protocol, job, frame, spool, startup, Python,
design or rubric edit occurred. No branch/worktree/commit/push, target deletion,
dependency download, default/AWS build, real secret/.env read, network/cloud call
or production provider was used. Observed starting disk headroom was 2.5 GiB.

Final tooling: the first strict library Clippy run failed only on unused
`nested_record` and `nullable` callbacks after their allowances were removed.
Restored their exact pre-existing item allowances, which the production-library
lint still requires for the derived wire callbacks. No function body changed.
The second strict library Clippy passed without diagnostics in 13.05s. Passing
behavioral suites were not rerun for this allowance-only correction.

`rustfmt --check --edition 2024 --config skip_children=true` passed on all four
owned Rust files. Scoped `git diff --check` and direct byte checks passed for
all five owned files, including untracked files, with final newlines and no
trailing whitespace. Nine copied vector constants match credential test source
byte-for-byte. Read-only snapshot diff confirmed the described wire/auth delta.
Observed final disk headroom remained 2.5 GiB; no resource blocker occurred.

Cargo/build and Rust implementation ownership is RELEASED to the parent.
Independent specification then quality reviews remain parent-owned.
