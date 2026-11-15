# DN-C08a strict startup inputs

Implemented A11 assignment 1 only, using the authoritative distributed contracts,
accepted DN-A11 adoption, A05 configuration rules, C04a1/C04a2a notes and existing
codecs. No applicable AGENTS.md exists in the workspace or ancestor directories.
Applied the test-driven-development and unslop skills. Shared pre-existing work
was preserved. No delegates, branches, worktrees, commits or pushes were used.

## Private API and files

`cluster::startup_config` is crate-private. Its two entry points are
`decode_api(&[u8]) -> Result<ApiConfig, ConfigurationError>` and
`decode_bootstrap(&[u8]) -> Result<Bootstrap, ConfigurationError>`.
Both return concrete records with value-free Debug and no Serialize or public
facade. ApiConfig contains cluster_id, bind, advertise_url, auth_file, validated
Template and expanded lifecycle Timing. Bootstrap contains cluster_id, node_id,
required nullable request_id, nonnull advertise_url, enrollment_token and
validated Template. UUIDs are canonical lowercase hyphenated uuid::Uuid values.
Version is validated as the exact unsigned integer token 1 before returning.

New files are `server/src/cluster/startup_config.rs`, its private
`startup_config/tests.rs`, and this note. `cluster/mod.rs` adds only the private
module declaration with a temporary dead_code allowance. Remove that allowance
when startup callers arrive. Existing configuration helpers `listen`, `api_url`
and `input`, and member deserializers `uuid` and `nullable_uuid`, now have
crate-private visibility. Their bodies and existing public behavior are unchanged.
No serializer or control-root generation/fixture change was needed.

Fixed typed object decoding operates directly on original bytes. Nested Storage
uses the existing RawValue options parser, preserving duplicates until driver
selection. Required nullable fields remain required, unknown and duplicate keys
fail at every object level, and positional arrays cannot substitute for records.
No Value map or duplicate-loss normalization precedes decoding. Both inputs have
a 128 KiB original-byte cap including whitespace, then the shared maximum-depth
32 lexical precheck before serde or RawValue. Integers reject bool, negatives,
fraction/exponent spellings and overflow. Serde diagnostics never escape.

API worker fields are all required. An absent lifecycle or empty object expands
Timing::default; any subset of its eleven exact fields overwrites those defaults
with exact positive integers, then Timing validation checks cross-field rules.
Explicit null fails. Bootstrap accepts manual request_id null and managed UUID
inputs, but always requires a nonnull advertised URL. It accepts none of API's
bind/auth_file/lifecycle fields, historical backends or extra secret fields.

Template validation supplies its bounded 64 KiB canonical encoding, API URL count,
capacity/queue ranges, bind syntax, PartitionSize parsing and maximum, storage and
namespace guards, and S3 feature refusal. Unknown drivers fail through the strict
Storage decoder. Startup adds nonempty literal paths bounded to 2048 UTF-8 bytes
with no NUL, and HTTP/HTTPS agreement across TLS mode, advertised URL and all API
URLs. Advertised/API URLs use the existing root-only endpoint guard. Bind/listen
allow port zero; advertised ports do not. Strings remain unchanged, including
relative paths, environment-looking text, tilde and Unicode. Storage HTTPS uses
its independent CA hint. No expansion, lookup, path resolution or file open occurs.
Enrollment tokens preserve exact bytes and require 43..4096 bytes in ASCII
33..126; parsing cannot establish their entropy.

## Validation evidence

Every Cargo command ran sequentially in the existing target with this prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

The focused command was:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::startup_config::tests
```

Before implementation, rejecting decoder stubs produced a meaningful initial red:
three groups failed on valid records, HTTPS input and bounded valid bytes; one
rejection group passed. After implementation all four groups passed. Additional
exact-boundary assertions initially exposed an arithmetic error in test-fixture
sizing, which was corrected without a production change.

The four groups cover API/bootstrap/manual and managed identities, default and
subset lifecycle expansion and invalid relations, representative nested strictness,
required nulls, integer spellings, identities, URL/TLS/capacity/feature/path rules,
token syntax limits, original-byte caps, shared depth boundary, canonical Template
size boundaries, sanitized Display errors and value-free Debug with sentinel
secret/path/identity checks. Template-size fixture values only generate decoder
input bytes; no construction API was added for tests.

Existing helper bodies did not change; prior C04a1/C04a2a codec test evidence is
reused. No broad suite or existing integration target was rerun.

Final focused result: four passed, zero failed, 42 unrelated tests filtered,
0.05s test execution. Strict library Clippy passed in 8.21s:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
rustfmt --edition 2024 --check server/src/cluster/startup_config.rs server/src/cluster/startup_config/tests.rs server/src/cluster/configuration.rs server/src/cluster/member.rs
rustfmt --edition 2024 --check --config skip_children=true server/src/cluster/mod.rs
```

Owned rustfmt checks passed. Tracked diff whitespace checks, direct trailing
whitespace scans and `git diff --no-index --check /dev/null PATH` for all three
new files reported no whitespace errors. No production changes followed these
checks. The no-index commands return 1 for new content with no error output.

## Boundaries and handoff

There are no implementation blockers or additional required file seams.
C08b still owns path resolution, protected bounded opens, auth parsing, credential
and TLS material validation, backend identity and fingerprints. Its libc
prerequisite was not added. C08c still owns generation-2 Config codecs, fixtures
and semantic attachment. Current control-root generations/fixtures are untouched.
No dependency edits/fetches, target deletion, default AWS build, storage access,
cloud calls, .env/real-secret reads, Python/CLI/runtime/listener/IPC/callback
implementation occurred. Synthetic sentinel strings are the only credentials
used in these tests.

Parsing proves no material identity, entropy, secret independence, certificate
validity, attachment, real Rust startup, enrollment eligibility, provider or
distributed behavior. Nonnull request IDs require later managed-launch validation;
null request IDs require later external enrollment validation. TLS scheme checks
establish local record consistency only. No DNS resolution or material loading
is implied. Independent specification and quality reviews remain parent tasks.
Cargo/build ownership is explicitly released at delivery.
