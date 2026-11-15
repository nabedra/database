# DN-C08b2b retained HTTP startup composition

Implemented adopted A14 assignment 2 only. Read the shared contracts and C08
rubric, adopted A11/A14, reviewed C08a/C08b1/C08b2a notes, startup decoders,
Reader/material records, identity helpers, Template/Storage and HttpStore/Namespace.
No applicable AGENTS.md was found. Applied TDD and unslop directly, without
children. Existing shared uncommitted work is preserved. No rubric boxes changed;
independent specification review, then quality review, remain with the parent.

## Files and concrete private API

Added startup_material/composition.rs, composition/tests.rs and this note.
startup_material.rs declares composition and updates its module description.
All composition types/functions are crate-private; the HTTP material record and
Reader-taking helpers stay module-private.

* prepare_api(&Path, std::time::Instant) returns ApiPrepared. It retains the
  original LoadedInput<ApiConfig>, Auth, optional TlsMaterial and HttpMaterial.
* prepare_bootstrap(&Path, std::time::Instant) returns BootstrapPrepared. It
  retains LoadedInput<Bootstrap>, including its own parsed enrollment token,
  optional TlsMaterial and HttpMaterial. It has no auth/lifecycle identities.
* ApiPrepared::connect(self) returns ApiReady with the preparation bundle,
  ConnectedHttp and authentication/lifecycle fingerprints.
* BootstrapPrepared::connect(self) returns BootstrapReady with the preparation
  bundle and ConnectedHttp.
* ConnectedHttp retains the actual HttpStore plus storage/worker fingerprints.
  HttpMaterial retains the exact accepted base string, Namespace, HttpToken and
  optional CaMaterial. Ready bundles continue to own all loaded material.
* StartupError currently has only InvalidConfig, Display "invalid_config",
  fixed Debug "InvalidConfig" and no error source. It has no underlying errors,
  offending paths, digests or values. Future provider access/timeout handling in
  C08b2c must add storage_unavailable at that boundary. No access occurs here.

Each public preparation entry creates exactly one Reader with the original
absolute CLI path and deadline. The input, auth, token, TLS and backend CA all
use that Reader's existing cache, aggregate limits and deadline checks. Original
input bytes and all Template strings remain intact. No persisted Template or
Control is opened. Preparation performs local loading only; connect consumes
retained records without reopening their paths. HttpStore::new receives the
retained token value and optional retained PEM, including Some for selected
HTTPS system roots. Supplied CA is also validated and bound for HTTP. Only
HTTP with no supplied CA has null trust. Actual constructor success precedes
identity computation. Identity helpers use those same material records and the
adapter's existing trailing-slash normalization. Loaded roots and CertifiedKey
remain available for later server setup. Bundles implement value-free Debug and
have no Serialize implementation.

files.rs changes are limited to removing consumed Reader/Auth allowances and a
cfg(test) optional temporary system-candidate list used by http_ca. Its ordinary
candidate order and all parser, read, DER validation and fixture bodies are
unchanged. Existing allowances for retained record fields still awaiting runtime
consumption remain. identity.rs removes consumed HTTP/auth/lifecycle/worker and
secret/Role allowances; its hashing/parsing bodies are unchanged, and S3's
pending caller allowance remains.

storage/http.rs adds only a cfg(test) field recording the exact optional CA
constructor argument and a cfg(test) declaration of http/startup_tests.rs. That
adapter-private file inspects the actual store's base, namespace, authorization
header, sensitive-header flag and exact CA argument. No production behavior or
production observation facade changed. Tests call this observation after actual
construction. Constructor success alone is not the material-use assertion.

The S3 branch explicitly returns invalid_config, including in a feature-enabled
build of this composition module. C08b2c must supply provider resolution, endpoint
policy and effective region before this slice can construct S3. There is no dummy
client, invented region/identity, new ObjectStore abstraction or AWS dependency
change. Feature-enabled compilation was not run here.

## TDD and focused evidence

The first four composition groups were written against rejecting preparation and
connect stubs before implementation. The first run compiled successfully, with
one unused-import warning for the not-yet-used implementation imports. Three
material/construction scenarios failed behaviorally with InvalidConfig at their
preparation unwraps; one sanitized-error group passed against the stub. This was
an honest rejecting-stub red, not a compile-error red. Result: exit 101, three
failed, one passed, 112 filtered, 0.60 seconds execution after 19.56 seconds build.

After implementation the same four groups passed, exit 0, 0.64 seconds execution
after 20.45 seconds build. Three additional groups then checked constructor/S3/
namespace error mapping, protected bootstrap and literal symlink-relative base,
and invalid selected/supplied CA. These additional groups have passing evidence,
not a separate preimplementation red claim. Seven groups passed, exit 0, 0.46
seconds execution after 19.15 seconds build. After strengthening original
Template/auth-byte retention assertions, the final seven-group run passed,
112 filtered, 0.64 seconds execution after 19.53 seconds build.

Synthetic API/bootstrap scenarios prepare through the real Reader and remove
input/auth/token/CA/cert/key files before actual construction. The API scenario
replaces token and auth paths with different/malformed contents before connect.
The resulting store still has the original header and exact CA argument, and
fingerprints correlate with the same retained records. Original input bytes,
Template, literal hints and exact Template fingerprint survive. TLS checks after
deletion call CertifiedKey::keys_match, select a signing scheme and inspect
nonempty roots. Debug sentinel assertions cover secrets, private-key/certificate
markers and temporary paths. They do not claim certificate lifetime or hostname
validation or actual TLS negotiation.

Explicit and selected-system-equivalent HTTPS roots use only owned temporary
candidates, including an absent first candidate. Both actual clients receive the
same retained PEM, with matching storage/worker identities and differing exact
Template identities. HTTP tests cover supplied CA and null CA separately. The
bootstrap symlink scenario resolves token hints beside the original CLI symlink,
not beside its remote target, and rejects a group/other-readable target.
Expired local deadline, relative input path, token cap, malformed selected CA,
malformed supplied HTTP CA, invalid namespace, constructor rejection and S3
rejection all remain invalid_config with fixed source-free diagnostics.

The existing reviewed create_tls_fixture is reused unchanged. It owns direct
OpenSSL children and tears them down before assertions. Each new fixture owns
its temporary directory with Drop. No listeners, HTTP requests, machine trust
store reads or sleeps as proof were used.

Every Cargo command used the existing target and this exact environment prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Exact Cargo commands after that prefix:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::startup_material::composition::tests
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::startup_material::files::tests::original_input_decoders_and_retained_bytes
```

The composition command ran four times with the red/green results above. Strict
production-library Clippy passed, exit 0, 9.20 seconds. The existing retained-input
check is justified by Reader's new cfg(test) initialization and protects ordinary
API/bootstrap loading alongside the injected-candidate path. It passed, exit 0,
one test, 118 filtered, 0.01 seconds execution after 1.10 seconds preparation.
No full loader
rejection matrix, identity suite, broad suite or test-target Clippy was rerun.

Scoped formatting command, used to format and then with --check to verify:

```sh
rustfmt --edition 2024 --config skip_children=true --check server/src/cluster/startup_material.rs server/src/cluster/startup_material/composition.rs server/src/cluster/startup_material/composition/tests.rs server/src/cluster/startup_material/files.rs server/src/cluster/startup_material/identity.rs server/src/storage/http.rs server/src/storage/http/startup_tests.rs
```

Scoped git diff --check passed. Direct trailing-whitespace/final-newline checks
include all seven Rust files and this new note, including untracked files.

## Limits and delivery

This establishes local material preparation, actual HTTP constructor validation
and retained token/trust consumption. It establishes no backend-access success,
listener startup, TLS networking, persisted attachment, generation-2 comparison
or real distributed startup. S3 is pending C08b2c; incompatible_cluster and
Control/template comparisons are pending C08c. Runtime wiring must consume these
bundles and later add listeners without reopening source material. Pending-runtime
item-level allowances are explicit; there is no module-wide new allowance.

No branches, commits, pushes, dependency edits/fetches, target deletion, AWS or
default-feature builds, cloud/network calls, .env/real-secret/host-trust/AWS-profile
reads or unrelated edits occurred. No implementation blockers remain in this
assigned HTTP slice. Cargo/build ownership is explicitly released at delivery.
