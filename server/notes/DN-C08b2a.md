# DN-C08b2a fixed loaded-material identities

Implemented adopted A14 assignment 1 only. Read distributed-contracts, the C08
rubric, adopted A11/A14, completed C08a/C08b1, startup decoders, material loader,
canonical writer, HttpStore normalization, Namespace and PartitionSize. No
applicable AGENTS.md was found. Applied test-driven-development and unslop;
performed this work directly without children. Shared existing changes remain.

## Files and private interface

Added `src/cluster/startup_material/identity.rs`, its private `identity/tests.rs`
and this note. Added only `pub(crate) mod identity` in startup_material.rs.
Configuration's existing `bounded` function gained crate-private visibility;
its body, Template/Timing codecs and exact Template fingerprint are unchanged.
The only fixture-loader edits are cfg(test) module visibility in files.rs and
create_tls_fixture visibility in files/tests.rs. No fixture or loader body changed.

All new callable interfaces are crate-private:

* `secret(Role, &[u8]) -> Result<Fingerprint, ConfigurationError>`, with Role
  variants Admin, Data, Enrollment and Http.
* `http(&str, &Namespace, &HttpToken, Option<&CaMaterial>)` and
  `s3(&str, &Namespace, &str)` return the same result type. S3 arguments are
  bucket, normalized namespace and effective region; this pure function grants
  no endpoint-policy authority.
* `authentication(Uuid, &Auth)` and `lifecycle(&Timing)` return that result type.
* `worker(&Template, &Fingerprint, Option<&TlsMaterial>)` returns that result
  type, using parsed PartitionSize bytes and the supplied retained material.
* `Fingerprint::as_str()` permits later fixed records and C08c to consume the
  lowercase hex. Fingerprint has value-free Debug and equality, no Display or
  Serialize. Its inner string stays private.

The entry functions and Role have temporary item-level dead_code allowances
until C08b2b/C08b2c callers arrive. No module-wide allowance was added. Inputs
and loaded material gained no Serialize. Only private fixed wire records
serialize, with recursively lexical fields and required nulls. Hashing appends
one NUL to the exact label and uses the existing sha2 dependency.

Every complete JSON encoding goes through the actual existing bounded writer
with a 16 MiB limit including syntax. Certificate lists retain references to
DER, sort/deduplicate roots and retain original chain order. Serialization
allocates hex for at most one bounded certificate at a time. There is no
complete DER-hex JSON tree, Value serializer or new framework. HTTP removes
only trailing '/' characters from base_url and uses Namespace's one final slash.
Tokens retain exact effective bytes, including the loader's single-LF removal
for HTTP only. Keys and path hints never enter semantic records.

## Validation

All Cargo commands used the existing target and the exact environment prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Before implementation, secret_fixed_vector failed behaviorally against a
rejecting stub returning an empty fingerprint. The expected fixed digest was
7b453f0cb92fd6a3831da14d64a10217fcbf15b679e476071afbad53ad8308a2.
There were no compile-failure red results. The first five groups existed before
the complete implementation. Two follow-up groups strengthened concrete-function
and local-field coverage, with additional assertions afterward.
Canonical JSON and hash literals were fixed independently using Python's stdlib
hashlib/json with explicit ordered records, not calculated by the Rust helper
under test. The vector tests exercise the production label constants.

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::startup_material::identity::tests
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_configuration round_trip_and_canonical_fingerprint_preserve_complete_outgoing_template
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_configuration constructed_candidates_enforce_paths_capacities_and_encoded_limits
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
rustfmt --edition 2024 --check --config skip_children=true server/src/cluster/startup_material.rs server/src/cluster/startup_material/identity.rs server/src/cluster/startup_material/identity/tests.rs server/src/cluster/configuration.rs server/src/cluster/startup_material/files.rs server/src/cluster/startup_material/files/tests.rs
```

Seven focused identity groups cover every label's literal canonical encoding
and fixed hash, role-separated secrets, required nulls, nested TLS order,
decimal bytes, exact writer cap and cap+1, HTTP spelling/explicit ports,
namespace normalization, region changes, ordered API URLs, listen and every
capacity, equivalent partition bytes and exact Template path differences.
Reader-loaded synthetic material covers root permutation/duplicates/PEM
whitespace, changed roots, chain extension/order/leaf differences, equivalent
PEM/DER private-key encodings, relocated TLS hints, loaded HTTP tokens and
single-LF handling, changed auth secret/cluster identity, lifecycle differences
and API-local bind/advertise/auth-file exclusion. Existing malformed loader
matrices were reused, not copied or rerun. The existing bounded test fixture
helper generated local synthetic certificates; no host trust bundle was read.

Final identity result: seven passed, zero failed, 98 unrelated tests filtered;
2.93 seconds execution after an 18.32 second build. Each existing writer check
passed individually, six unrelated tests filtered each. Strict library Clippy
passed in 9.16 seconds. Owned rustfmt passed. Direct trailing-whitespace and
no-index whitespace checks passed for all seven files, including untracked ones.
The no-index commands return 1 for new content and emitted no errors. An initial
whitespace-check script used a subprocess option unsupported by the installed
Python 3.6; the compatible rerun passed. No Rust compile failures occurred.

## Limits and handoff

Production identity functions perform no file or network I/O. C08b2b must
validate actual HTTP construction and pass these same retained material records;
C08b2c must establish the standard S3 policy independently before using its pure
hash. These functions expect already validated local inputs and retained TLS
matching the local hints. They do not establish attachment, generation-2 codec,
configuration wiring, runtime startup, TLS networking or provider behavior.
Bootstrap can call storage/worker and its enrollment secret helper; no invented
authentication or lifecycle bundle exists. C08c owns attachment and generation 2.

No branch, commit, push, cloud/network operation, .env or real secret access,
host AWS profile read, dependency edit/fetch, target deletion, AWS build or broad
test run occurred. No runtime, adapter constructor or other production changes
were made. Cargo/build ownership is explicitly released at delivery.
