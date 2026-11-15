# DN-R03a2a: memory-owned keys and fixed signing domains

Implemented only the signing-key/fixed-domain portion of adopted A15 mini 2.
R03a2b retains signature/content/authority proof validation. Full R03a2, replay,
HTTP, enrollment transitions and runtime lifecycle remain incomplete. Signatures
here prove possession and bytes, never permission, membership or TLS identity.

Read shared contracts, R03/C03 and testing rubric, adopted A07/A15 credentials,
A11 retained secret constraints, R03a1 including the parent checked-construction
correction, actual wire/accessor implementations, current manifest/lock, member
Session shape and cached ring 0.17.14 signing/HKDF APIs. Applied TDD and unslop.
No applicable AGENTS.md was found in checked ancestors or scoped directories.
This child did the work without delegation.

## Exact scope and dependency delta

New `src/node/auth/credentials.rs`, its private `credentials/tests.rs`, and this
note. `auth.rs` adds a private credentials module and crate-private exports of
`WorkerSigningKey`, `ApiSigningKey`, `worker_verifier`; updates the description;
and separates consumed wire reexports from pending-caller allowances.
`wire.rs` adds only `EnrollmentUnsigned::public_key() -> Result<[u8;32], Error>`
and removes the consumed allowance on `Sender::public_key`'s impl. Its private
DecodeRecord/nested callbacks, fields, bounds, validation/encoding bodies and
canonical formats are unchanged. `wire/tests.rs` adds only a fixed-key assertion
in the existing canonical/enrollment group. The negative Deserialize guards and
all prior tests remain intact. A byte snapshot comparison of wire.rs confirmed
exactly those accessor/allowance changes.

Added direct unconditional `ring = "=0.17.14"` to `server/Cargo.toml`, independent
of s3. The only task delta in `server/Cargo.lock` is `"ring",` in the
meshdb-server root dependency list. A pre-edit lock snapshot comparison confirms
no package/version/checksum/dependency resolution change. Locked/offline commands
accepted the hand-edited root entry. Shared existing manifest/lock changes were
preserved. No fetch, network/cloud call, default/AWS build, target deletion,
separate target directory, real secret/.env read, branch, worktree, commit or
push occurred. No Control/root, Python, startup, HTTP, controller or frame code
was changed. A broad read-only AGENTS discovery was stopped; it found only
sibling-project guidance, which does not apply here.

## API and caller limits

Both key structs own a ring Ed25519KeyPair, have private fields, no Clone or
Serialize/Deserialize, and value-free Debug of exactly the type name. Neither
exports seed/PKCS8 material or arbitrary signing. Public keys return `[u8;32]`;
signatures return `Result<[u8;64], auth::Error>`. Errors map to static source-free
Invalid/Limit without values or sources. Seeds remain memory-only; there is no
memory zeroization guarantee.

- `WorkerSigningKey::new()` fills one 32-byte seed with
  `SystemRandom::new().fill`, then uses `from_seed_unchecked`. Only cfg(test)
  exposes a private deterministic `from_test_seed([u8;32])` constructor.
- `ApiSigningKey::derive(cluster: Uuid, admin_token: &str)` consumes exact retained
  admin UTF8, without reopening files or consulting fingerprints/configuration.
  It enforces the A11 invariant of 43..4096 bytes, each ASCII 33..126, before
  hashing. Salt is cluster.as_bytes(), extract input is the exact admin bytes,
  expand info is `b"meshdb.api-signing.v1"` with no NUL and Len32 output, then
  Ed25519 from that seed. It retains the cluster and rejects signing for another
  cluster. Caller must supply the retained admin field, never data/enrollment
  credentials or their fingerprint. Syntax establishes no entropy; deployment
  still requires at least 256 generated bits in the admin secret.
- Worker `sign_enrollment(&EnrollmentUnsigned)`, `sign_request(&Auth)` and
  `sign_response(&ReplyAuth)` sign the checked bounded encoding/canonical bytes.
  API `sign_request(&Auth)` does the same for API role. Each checks the record's
  sender public key against its own; request/reply signing checks the role.
  API process UUID is not a derivation input. Worker keys have no pre-associated
  cluster/identity; the later boot owner retains key/incarnation/session across
  enrollment retries and associates claims with its boot identity.
- Signing prefixes are exactly `b"meshdb.enroll.v1\0"`,
  `b"meshdb.http-request.v1\0"`, `b"meshdb.http-response.v1\0"`. The private helper
  accepts only a closed private domain enum. It checks 16 KiB enrollment / 4096
  auth byte caps before allocating prefix+body. Input encoders already enforce
  their limits before returning canonical bytes. No external unbounded buffer
  or arbitrary domain enters a signing method.
- `worker_verifier(cluster: Uuid, node: Uuid, incarnation: Uuid, session: Uuid,
  public_key: &[u8;32]) -> [u8;32]` is the exact pure function for later claimed
  keys. It hashes `b"meshdb.worker-verifier.v1\0"`, four Uuid::as_bytes() values
  in that order, then raw key bytes with ring SHA256. The result is raw digest
  bytes. Later existing Session construction converts these bytes to exactly
  64 lowercase hex digits. It is a public binding, never a bearer or MAC.

Pending boot/proof callers justify item-specific dead-code/reexport allowances.
Mixed wire impl allowances remain for still-unconsumed decoders/accessors; no
module-wide suppression was added. No verifier, opaque proof, checked content,
fresh Control authority, replay state, side effect or supervisor was implemented.

## Independent expectations and tests

The seed `9d61...7f60` and public key `d75a...511a` are copied from cached ring
0.17.14 `tests/ed25519_test_private_key.bin` and
`tests/ed25519_test_public_key.bin`. They match published RFC 8032 section 7.1
test 1. The pinned empty-message signature `e556...100b` is that published test's
signature and was independently reproduced with installed Python cryptography
40.0.2 Ed25519PrivateKey, then verified directly by ring in the test. The cached
crate's ed25519_tests.rs references BoringSSL text fixtures, but those text files
are absent from this cache; no claim is made to have copied their contents.

Python stdlib hmac/hashlib independently computed HKDF with cluster UUID integer
1 encoded as 16 big-endian bytes and synthetic admin `A` repeated 43 times:
`prk = HMAC_SHA256(cluster_bytes, admin_bytes)` and
`seed = HMAC_SHA256(prk, b"meshdb.api-signing.v1" + b"\x01")`.
Pinned seed is `b36a6d0f436b1e08393c0c9f65ea850039debec28a69c5971512125eb3e6f625`;
Python cryptography independently derived public key
`41f19e73e483776f4b552779a73e361242210c5a26b04825db62e69b52db0d95`.
The verifier literal is Python hashlib.sha256 over its exact prefix, UUID integers
1/3/4/5 as UUID.bytes, then the copied raw worker public key:
`1214ed9c37ee28fe7e9af7c1731deef2dedd682bfdf2979f81e3fa3b286f11e8`.

All four domain/canonical signature literals were generated once offline using
Python cryptography Ed25519PrivateKey.sign(prefix + literal.encode()). Enrollment,
API readiness request and worker reply reuse R03a1 literals with synthetic public
keys substituted. The worker dependency-open request adapts that literal's closed
route/claims using Python insertion-order JSON encoding. Complete resulting JSON
and signatures are pinned in Rust constants. Expected bytes never come from the
production encoder/signing function. A final independent Python check reproduced
HKDF, API public key and all five signature literals, including the published
empty-message case. Python ran synchronously with no spawned child processes,
installs or real keys; the short generator was outside the repository.

Four focused groups check published crypto, repeatable API derivation, different
cluster/secret, admin input bounds, API process-ID independence, fixed verifier
and each UUID/key change, all four exact typed signatures, local role/key/cluster
rejection, altered bytes/key/domain, absent/double NUL rejection and changed
sequence. Direct ring verification checks exact prefix+literal and rejects altered
inputs. Compile-time guards reject Clone/Serialize/Deserialize on both key types.
Debug/errors are value-free. Private signing cap tests check cap/cap+1. Random
construction is only a narrow successful-construction/length check and source
inspection of one fill, not statistical entropy evidence. Alternate synthetic B/C
secret derivations differ; the API takes only admin text and has no data/enrollment
input. Existing codec matrices were not duplicated, nor is fresh authority claimed.

## Red/green and commands

Tests preceded implementation. First run against an empty module failed compilation
for missing APIs, exit 101; that is not behavioral red. Compiling scaffolding then
used zero signature/digest outputs and replaced the HKDF-derived seed with zero.
The same three groups compiled in 21.72s and failed behaviorally, exit 101, at the
fixed verifier, fixed API public key and fixed enrollment signature assertions.
Removing those placeholders yielded three passes in 0.01s after 20.97s compilation.
The cap group, negative trait guards and extra direct wrong-key assertion were
added afterward; no separate behavioral-red claim is made for them or for later
assertions not reached before earlier failures. Expected literals were unchanged.

Every Cargo invocation used the existing target and this exact environment:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Commands, each with that prefix:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::credentials::tests
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::wire::tests::literal_canonical_records_and_enrollment_reconstruction -- --exact
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::wire::tests::validated_records_require_checked_construction -- --exact
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
```

Final credentials run: four passed, 127 filtered, 0.01s after 21.54s compilation.
Accessor/canonical regression: one passed, 130 filtered, 0.00s.
Checked-construction regression: one passed, 130 filtered, 0.00s. These two cases
justify the wire regression scope through the new accessor and typed-signing trust
boundary. Strict production-library Clippy passed in 10.20s without diagnostics.
No --tests/all-target lint or unrelated suite ran; the parent's six known unrelated
Fake/payload test lint diagnostics remain untouched.

`rustfmt --check --edition 2024 --config skip_children=true` passed for the five
owned Rust files. Scoped Git diff whitespace checks passed. Direct byte checks
cover all eight owned files, including untracked Rust/note files, for trailing
whitespace and final newline. Final source/lock snapshot checks confirmed the
narrow changes described above. Shared work was preserved.

Cargo/build and Rust-implementation ownership is explicitly RELEASED to the
parent after these checks. Independent review remains the parent's responsibility.
Stop here: R03a2b and full cryptographic authority remain unimplemented.
