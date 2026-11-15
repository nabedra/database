# DN-C08b1 local file and TLS material

Implemented only local synchronous A11 material loading in the shared main
checkout. Read distributed-contracts, the relevant distributed-nodes rubric,
adopted DN-A11, completed DN-C08a, actual configuration/startup parsers, HTTP
adapter and cached rustls PEM/signer source. No applicable AGENTS.md was found
in the workspace or ancestors. Applied test-driven-development and unslop.
No delegates, branches, worktrees, commits or pushes were used. Existing shared
changes and the parent's Cargo prerequisite were preserved.

## Files and private interface

New files are startup_material.rs, startup_material/files.rs, its owning-module
files/tests.rs, and this note. cluster/mod.rs adds its crate-private declaration.
startup_config.rs changes only MAX_INPUT_BYTES to pub(crate), sharing the existing
128 KiB input bound without duplicating either decoder. Cargo files are unchanged
by this assignment. No public SDK or API changes.

Reader::new takes the absolute original CLI input path and the original
std::time::Instant deadline. It retains the literal parent directory, without
canonicalizing the input symlink. Relative hint strings join that directory;
absolute strings stay absolute. There is no expansion or Template mutation.
api() and bootstrap() return LoadedInput with original bytes and the existing
parsed ApiConfig/Bootstrap. Bootstrap is a protected read. auth(), http_token(),
http_ca(https, supplied_hint), and tls(&local_tls_hints) return concrete retained
records. These methods are crate-private; raw read and candidate injection helpers
remain private. No persisted-template reader is introduced. C08b2 must pass only
local input hints and must never open persisted foreign Template paths.

Auth retains original bytes and exact three parsed distinct tokens. It uses
configuration::input and deserialize_object on original bytes, including the
shared depth check and serde duplicate detection. HTTP token removes only one
optional final LF. Records expose no Serialize and have value-free Debug,
including Reader, cache entries, rustls roots and provider-owned key wrappers.
Errors are existing fixed ConfigurationError categories with no sources or values.
Deadline exhaustion uses LimitExceeded; later startup-envelope mapping is pending.

Reads validate regular type before open and on the opened descriptor. Unix opens
set libc::O_NONBLOCK before descriptor checks. Secret descriptor permissions have
zero group/other bits. Symlinks to regular files and read-only owner-readable
mounts work without chmod. All reads probe cap+1 despite metadata and charge a
checked 8 MiB aggregate budget, with at most one aggregate overflow probe byte.
Deadline checks surround I/O and validation and run between chunks/certificates.
This does not preempt uninterruptible filesystem operations.

A narrow 16-entry per-startup retained-byte cache reuses literal resolved paths,
including their opened-descriptor permission decision. It stays within the same
aggregate byte budget, retains no failed/empty reads, and does not reopen a cached
path. A later stricter secret request checks its original permission decision.
Different path spellings are not canonicalized into aliases. Callers retain each
returned material record rather than rereading it for identity/runtime use.

TLS retains original chain/key bytes, selected CA PEM, sorted/deduplicated CA DER,
rustls roots, ordered chain and provider-owned CertifiedKey. Rustls parses every
certificate, validates roots, loads the key and explicitly verifies leaf/key
match. Certificate limits are 64 per chain, 1024 per CA bundle before dedup,
64 KiB per DER; chain/CA file caps are 1 MiB and key cap is 256 KiB. Strict PEM
envelope validation rejects unknown labels, junk, nested/truncated sections and
malformed trailing input rather than accepting only the first usable item.
Supported PEM key encodings and DER keys use existing rustls/provider APIs.
Supplied HTTP CA is validated even for plain HTTP; plain HTTP with null CA returns
None. HTTPS null CA selects the first readable regular candidate in the adapter's
specified three-path order. After successful open, empty/read/cap/parse failures
are terminal and cannot silently select another trust bundle. Retained PEM is
ready for later Some(&bytes) HttpStore construction; no constructor/network call
is made here.

## Validation evidence

All Cargo invocations used the existing target, sequentially under the sole
Cargo slot, with this prefix and flags:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
--manifest-path server/Cargo.toml --locked --offline --no-default-features
```

Exact focused command:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::startup_material::files::tests
```

Initial test preparation incorrectly assumed tempfile was available. That compile
error was corrected with an owned UUID temporary-directory helper, without a
dependency edit. Before production reader implementation the focused command
then failed on a valid literal-path read against the rejecting stub: one failed,
zero passed. This is the observed initial behavioral red. Auth/file tests were
written before their implementation; the TLS and input integration groups were
expanded after implementation and do not have separate observed preimplementation
red evidence. An intermediate compile error using CertificateDer::sort was fixed
by explicitly comparing DER byte slices. The final material command passed all
six grouped tests, 70 unrelated tests filtered, 0.71s execution.

Coverage includes literal spaces/$/tilde paths, absolute paths, input symlink base,
owner-read-only secret symlinks, permission refusal, directory/device/FIFO refusal,
and a deterministic regular-to-FIFO replacement between precheck and open. The
swap succeeds in reaching descriptor refusal without a watchdog proving safety.
A growing regular file exercises the read bound despite smaller pre-open stat.
Expired and crossed deadlines, exact/max+1 file caps, exact aggregate capacity
and the next-byte refusal use the real reader. Auth/token cases cover exact
preservation, duplicate/unknown keys, invalid UTF-8, wrong shapes/depth/trailing
input, invalid characters/lengths, pairwise equality and sanitized diagnostics.
API/bootstrap tests use existing decoders, protected bootstrap permissions,
original byte retention and API cap boundaries.

TLS tests import scripts/test_tls_material.py to create owned disposable CA/leaf
material and use controlled OpenSSL encoding conversions. Each child is owned,
polls to a fixed deadline, and is killed/reaped on expiry through its Child
handle; no numeric PID kill or background process is used. Tests verify actual
key match/mismatch, equivalent PEM/DER key material, retained bytes after file
replacement, malformed/truncated/trailing material, DER oversize, chain 64/65,
and CA 1024/1025 before dedup. The CA count test calls the same private parser
directly because this RSA fixture reaches the 1 MiB byte cap before 1024 roots;
it is not evidence of reading an over-cap bundle. Candidate selection tests inject
only synthetic paths, including missing/nonregular, invalid, empty and over-cap
first candidates. No machine trust store or real credential is read.

The essential affected parser check passed one existing grouped test:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::startup_config::tests::raw_byte_depth_template_bounds_and_sanitized_diagnostics
```

Strict targeted library Clippy passed in 7.38s:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
rustfmt --edition 2024 --check server/src/cluster/startup_material.rs server/src/cluster/startup_material/files.rs server/src/cluster/startup_material/files/tests.rs
rustfmt --edition 2024 --check --config skip_children=true server/src/cluster/mod.rs server/src/cluster/startup_config.rs
```

Owned formatting passed. git diff --check, direct owned-file trailing-whitespace
checks, and git diff --no-index --check /dev/null for new files passed. The first
whitespace wrapper used unsupported Python 3.6 capture_output; rerunning with
PIPE/universal_newlines passed. No repeated broad suite, default AWS build,
dependency fetch, target deletion, cloud call or .env/real-secret read occurred.

## Remaining work

C08b2 owns fingerprint serialization, fixed identities, effective backend
composition, storage adapter integration/provider resolution and removal of the
explicit item-level dead_code allowances as callers land. C08c owns generation-2
Config/schema/fixtures. Runtime/CLI/IPC, listener/client construction and lifecycle
ownership remain outside this slice. Tests establish local parsing and material
consistency, not credential entropy, certificate lifetime/hostname policy,
distributed attachment, real startup or network behavior. Parent specification
and quality reviews remain required.

Cargo/build ownership is explicitly released at delivery.

## Parent corrections after specification review

The reviewer found that provider key parsing can accept a complete PKCS#8
sequence followed by extra DER bytes, including when those bytes are inside a
single PEM block. The parent extended the existing TLS scenario with a matching
valid raw PKCS#8 baseline, then one appended byte and concatenated keys in both
raw and PEM encodings. PEM wrapping is checked against the exact input bytes;
its failure message exposes no key bytes. Before production correction all four
malformed keys were accepted: the regression failed with [false,false,false,false]
instead of [true,true,true,true], exit 101, 0.96s.

The loader now checks that the decoded key contains exactly one definite,
minimally encoded outer DER SEQUENCE before provider loading. Checked length
arithmetic rejects truncation, trailing objects/bytes, indefinite lengths and
nonminimal outer length encodings. Existing rustls/provider parsing still
validates the key contents and leaf match. The same check covers raw and
PEM-decoded keys; no new dependency or general ASN.1 parser was introduced.

The reviewer also found that killing the Python TLS generator on timeout could
strand its OpenSSL descendant. The Rust fixture now imports the existing Python
helper only to write its private fixture files and record its three OpenSSL
commands; that Python call spawns no OpenSSL process. Rust executes each command
directly through an owned Child. The shared wait helper kills and reaps that
direct child before returning a timeout or wait error. A new direct-child timeout
test asserts the TimedOut result and subsequent reaped status. The TLS scenario
executes the actual OpenSSL commands normally. Source inspection establishes
their direct ownership; this test does not claim to interrupt OpenSSL internally.
The shared scripts/test_tls_material.py file remains unchanged. There is no
observed failing-before descendant-leak probe for this fixture correction.

All Cargo commands used the same four resource environment variables and flags
recorded above. Exact verification after the parent changes:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::startup_material::files::tests::tls_real_material_retention_mismatch_and_strict_pem
Initial DER regression: exit 101, one failed group, 0.96s, as described above.
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::startup_material::files::tests
After correction: exit 0, all seven material groups passed, 0.81s.
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
Exit 0, 8.60s.
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::startup_material::files::tests::tls_real_material_retention_mismatch_and_strict_pem
After making the byte-equality assertion diagnostic value-free: exit 0,
one affected TLS group passed, 0.68s.
rustfmt --edition 2024 --config skip_children=true --check server/src/cluster/startup_material/files.rs server/src/cluster/startup_material/files/tests.rs
Exit 0; scoped git diff --check and direct whitespace/final-newline checks passed.
```

The parent changed only files.rs, files/tests.rs, this note and the rubric.
No runtime, shared fixture script, dependency or public API changes. Fresh
specification re-check precedes quality review. Cargo/build ownership is released.
