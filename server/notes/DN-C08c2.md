# DN-C08c2 compatible attachment and control-result errors

Implemented only adopted A11 assignment 3's comparison boundary, after C08c1's
Config generation 2 amendment. Read the shared contracts, C08/testing rubric,
A04, A05, A10, adopted A11/A14, completed C08 notes and actual codecs, store,
Reader, identities and retained HTTP composition. Applied test-driven-development
and unslop. No applicable AGENTS.md was found. Shared uncommitted and untracked
work is preserved. No delegation, branches, worktrees, commits or pushes occurred.

## Private API and owned files

New `server/src/cluster/attachment.rs` provides:

- `control_result(Result<LoadedControl, StorageError>) -> Result<LoadedControl,
  StartupError>` for actual C04b1 load/initialize results.
- `check_attach(LoadedControl, ApiReady, &str, Provisioning) ->
  Result<AttachedConfig, StartupError>`. The name is supervisor-validated. The
  explicit provisioning argument comes from callback declarations. Absent
  callbacks mean External; callers must not infer it from persisted state.
- Opaque `AttachedConfig` privately owns both original LoadedControl and ApiReady.
  There is no mutable root, replacement base, raw-digest constructor or backend
  factory. Debug retains the existing redaction of local material and tokens.

New private tests live in `attachment/tests.rs`. `cluster/mod.rs` adds only its
crate-private module declaration. `startup_material/composition.rs` adds fixed
StartupError variants/display strings and a read-only `ApiReady::matches_backend`
comparison against the private retained HTTP base/Namespace. Its construction,
loading and identity computation are unchanged. `startup_material/files.rs` adds
only a cfg(test) bounded read-attempt observation hook, and widens the cfg(test)
fixture module declaration to crate visibility so attachment can reuse the
existing crate-visible TLS fixture function. Fixture bodies are unchanged.
`identity.rs` needs no changes. This note is the only new delivery document.

## Comparison and error rules

The boundary checks cluster UUID, expected name, generation 2, explicit
provisioning, adapter contract, storage/auth/lifecycle and independent local
worker identities. Managed requires meshdb.callbacks.v1; External requires null.
It compares stored HTTP address and namespace with retained adapter declarations
using exactly trim_end_matches('/') and Namespace::new, preserving all other
accepted URL spelling. It checks TLS presence, stored exact Template hash against
that stored Template only, stored Timing hash, and stored worker settings hashed
with retained local effective storage and TLS identities. It never opens stored
credential or TLS path hints. Equivalent local path spellings, root PEM encodings
and normalized namespaces/base trailing slashes therefore attach. Underlying
CA ordering/key encoding equivalence remains the reviewed identity/Reader code.
API bind/advertise and auth path locations are excluded.

The returned base/token and outgoing Template are moved unchanged, with all
policy, revision, admin history pointers, controller, scheduler, nodes, intents
and retained identities. Comparison writes nothing and never runs new-admission
or status-size checks. No initial-nodes parameter is added; tests vary a later
initial candidate's policy and show that persisted policy wins.

control_result maps InvalidCatalog, UnsupportedCatalog and LimitExceeded to
incompatible_cluster. This is a precise bounded classification of C04b1 codec
and root-read limits, including generation 1 that cannot decode. The helper is
for loading, or initializing with an already validated candidate, not a local
candidate-validation API. Every other StorageError maps to storage_unavailable,
including absence, conflict and ambiguous outcomes. No unavailable/uncertain
result proves nonapplication or permits resending Create. Local preparation and
constructor errors retain invalid_config. All errors contain only fixed variants
and static display strings, with no sources, paths, material or digests.
C04b1 remains unchanged and owns the original deadline and one-Create behavior;
cancellation/deadline ownership remains with its caller.

All comparison/fingerprinting work is bounded and synchronous, intended for the
existing finite coordinator executor, outside short locks and Tokio polling.
The test-only observer uses thread-local storage for one synchronous closure,
a 32-attempt bound and an unwind cleanup guard. It records before metadata checks,
so even nonexistent path attempts are observable. A real failed Reader input
read is the positive control. There is no production I/O abstraction or process
wide mutable observation state.

## Tests and red/green provenance

The first three groups compiled against rejecting check_attach/control_result
stubs and failed behaviorally: compatible attachment and initial/racing roots
returned InvalidConfig; real generation-1 decode failures mapped to invalid_config
instead of incompatible_cluster. Exit 101, three failed, zero passed, 131 filtered;
build 30.44s, execution 0.02s. One unused test import warning was then removed.

The expanded five-group attempt exposed a test fixture module visibility compile
error. This is not claimed as behavioral red. After the production implementation
and cfg(test) access correction, all five groups passed, 131 filtered, 0.45s,
after 22.14s build. Subsequent strengthened tests have green-only provenance.
Six groups passed, 131 filtered, 0.97s after 21.64s build, including independently
changed backend CA, worker CA and matching-key/different-certificate material.

The final groups cover real Reader and HttpStore construction without requests;
two preparations in different owned directories; retained TLS/material after
original file deletion/replacement; changed PEM whitespace/duplicate roots;
nonexistent foreign token/cert/key/CA hints with zero attempts; preserved
noninitial policy, admin head/sequence, held controller, timers, node and intent;
independent local cluster/settings/backend/auth/lifecycle changes; valid-root
stored semantic/Timing/settings mismatches and codec-rejected exact hash mismatch;
initial creation, existing root and compatible/incompatible CAS race winners;
no second Create/reset; unchanged captured root/token; real generation-1 bytes
with/without worker identity through load and initialize; failed reads and expired
absolute deadlines. Tests use existing Fake unchanged through its test-only crate
alias and actual Control/Template codecs. No ordering sleeps or detached tasks
are used. Owned temporary directories and existing fixture child ownership handle
cleanup. The existing TLS fixture internally reuses its established Python
command-definition helper and owns/reaps its OpenSSL children; no new Python or
certificate-generation framework was added.

## Commands and validation

Every Cargo command used the existing server/target, with this exact prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Commands after that prefix:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::attachment::tests
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::startup_material::composition::tests
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib cluster::startup_material::files::tests::original_input_decoders_and_retained_bytes
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
```

The final production-code verification passed six attachment groups, 131 filtered,
0.92s after 20.86s build; seven composition groups, 130 filtered, 0.43s; and one
Reader regression, 136 filtered, 0.01s. Strict production-library Clippy passed,
exit 0, in 9.80s. Composition regressions protect
the extended error enum/accessor without changing construction. The single Reader
regression protects ordinary input loading with observation disabled. Identity
vectors and full parser/loader rejection matrices are reused, not repeated.
No test/all-target Clippy, default/AWS build or broad suite was requested/run.
The six known unrelated Fake/payload test lints remain parent cleanup.

A final test-only strengthening explicitly checks successful External attachment
and successful reattachment after a different new initial policy. The same six
attachment groups passed again, 131 filtered, 1.04s after 23.49s build. No further
production behavior changed. Scoped rustfmt --check, scoped git diff --check and
direct Ruby whitespace/newline checks all passed, including untracked files.

Scoped rustfmt uses `--edition 2024 --config skip_children=true`, on attachment.rs,
attachment/tests.rs, cluster/mod.rs, startup_material/composition.rs and files.rs,
then the same command with --check. Direct trailing-whitespace/final-newline
checks cover these files and this note, including untracked files.

## Deferred work and delivery

S3 remains explicitly unsupported by local composition until C08b2c provides an
actual retained client, region and standard endpoint decision. Stored S3 cannot
attach to this HTTP bundle; no identity/provider/backend is invented here.
Runtime wiring, initial policy assembly, listeners, IPC, A04 prepared/activate,
controller ownership and callback authority remain later work. Compatible
attachment grants none of them. Later runtime must complete comparison before
prepared and perform no effects before activate. These tests establish no CLI,
handshake, TLS negotiation, backend availability or network startup proof.
No dependency changes/downloads, target deletion, real secret/.env/host trust or
AWS profile reads, network/cloud calls or auth/credential implementation edits
occurred. Historical notes and the rubric are unchanged.

Cargo/build ownership is RELEASED at delivery for independent reviews.
