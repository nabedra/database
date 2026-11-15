# DN-C04a2a strict template and timing codecs

Implemented 2026-11-11 against authoritative `plans/distributed-contracts.md`, accepted DN-A05 and DN-A07, and the reviewed member codecs. No applicable AGENTS.md was found in the repository or ancestors. Applied local test-driven-development and unslop guidance. Existing shared-main changes were preserved.

## API and schema

`cluster::configuration` exports plain mutable `Template`, `Tls`, `Storage::{Http,S3}`, `HttpOptions`, `S3Options`, `Timing`, and sanitized `ConfigurationError::{InvalidRecord,LimitExceeded,UnsupportedStorage}`. Template and Timing expose `decode(&[u8])`, `encode()`, and `validate()`. Template additionally exposes `fingerprint() -> Result<String, ConfigurationError>`. Timing implements the full A05 defaults without I/O. Debug prints record names without their contents, including nested storage/TLS records.

Template has precisely the eight assigned fields. Nested records reject arrays, duplicate fields, missing fields including required nulls, unknown fields and inappropriate scalar types. Integer fields deserialize directly to u64 and reject fractional/exponent spellings. Storage dispatch preserves raw option bytes only until parsing the selected exact typed options record, so duplicate fields cannot disappear into a map. S3 returns UnsupportedStorage without the feature. A feature-gated valid S3 fixture is included but was not built in this slice.

Input bytes are capped before allocation at 64 KiB for Template and an explicit 4 KiB for Timing. A small lexical nesting precheck enforces depth 32, skips strings and escapes, and leaves JSON syntax validation to serde. It also covers opaque RawValue options before allocating them. Constructed encode validates all field constraints before writing through a capped writer. Validation includes encoded bounds. Output contains required nulls and preserves every original string, including API URL spelling, partition-size spelling and outgoing credential/TLS paths.

API URLs reuse the member endpoint syntax validator plus A07's empty-or-root path restriction. Member's endpoint helper gains only crate-private visibility; Node and Intent behavior is unchanged. Bind addresses permit port zero and validate hostname/IPv4 or bracketed IPv6 syntax without lookup. Partition sizes reuse PartitionSize. Namespaces reuse Namespace and the HTTP adapter's additional ASCII component restrictions. HTTP URL and S3 bucket checks match the adapter constructor guards locally because those constructors do not expose pure validators and invoking them would initialize clients. HTTP backend URLs retain their permitted prefix paths and explicit zero-port semantics separately from advertised API URLs. No outgoing paths are opened, resolved or checked for local existence.

Timing requires all positive fields, heartbeat < expiry, checked `2 * renew < lease`, and retry_initial <= retry_max. The multiplication avoids rounding odd lease values and fails closed on overflow. Checked future timestamp additions remain caller obligations.

## Fingerprint

The sole Rust serializer emits recursively lexical object keys by fixed record declaration order and Storage's explicit driver/options order. It emits compact UTF-8 JSON with decimal integers and all required nulls. The bounded canonical template bytes follow `b"meshdb.control-template.v1\0"` in SHA256 and produce 64 lowercase hex characters. Tests pin an independently calculated digest and verify input order/whitespace independence. Changing outgoing path strings changes this exact outgoing-template fingerprint.

This is not an attaching configuration, storage or auth fingerprint. Later attachment must compare loaded credential/trust identities, then retain and use the persisted outgoing template verbatim. A07 changes authentication semantics without adding template fields.

## Evidence

Tests were written before production code. The first Cargo invocation from the repository root failed because that directory has no manifest. The corrected initial focused run in server failed with E0432/E0433 because cluster::configuration did not exist. During implementation, sha2 0.11's digest array lacked LowerHex; formatting the finalized bytes individually corrected the compile failure.

All Cargo commands used existing server/target, offline/no-default-features, CARGO_BUILD_JOBS=2, CARGO_PROFILE_DEV_DEBUG=0, CARGO_PROFILE_TEST_DEBUG=0 and CARGO_INCREMENTAL=0. Every command after the dependency edit also used --locked. Final passing commands from the repository root were:

```text
cargo test --manifest-path server/Cargo.toml --offline --locked --no-default-features --test control_configuration
cargo clippy --manifest-path server/Cargo.toml --offline --locked --no-default-features --lib --test control_configuration -- -D warnings
rustfmt --edition 2024 --check server/src/cluster/configuration.rs server/tests/control_configuration.rs
rustfmt --edition 2024 --check --config skip_children=true server/src/cluster/mod.rs
```

Seven focused causal test groups pass. They cover ordinary HTTP templates and complete timing defaults, exact canonical round trips and pinned fingerprints, semantic/path changes, required nulls, duplicate/unknown/object/enum/integer rules, malformed authority/ports, API versus backend path rules and bind versus advertised zero ports, paths/namespaces/partition sizes/capacities, timing relations/overflow, bounded input/output and sanitized diagnostics. Direct whitespace checks passed for owned files and the member helper line.

The only dependency delta for this task is direct `sha2 = "=0.11.1"` plus `"sha2"` in meshdb-server's root package dependency list in server/Cargo.lock. Its existing locked package and transitive dependency entries were untouched. No fetch, new crypto implementation or other dependency edits occurred.

## Deferred obligations and handoff

The parent must reconcile future-stop reservation before C04a2b root admission. This slice computes no root reservations. Control/admin codecs, persistence, transitions, runtime/startup/config-file loading, semantic attachment, loaded credential/trust identities, auth/session signing, callbacks, framing and networking remain outside this implementation. No filesystem credential fixtures, network/cloud calls, .env reads, delegation, branch/worktree, commit/push, full suite or AWS build occurred. Independent specification and quality reviews follow under parent ownership. Cargo is released at delivery.

## Parent correction after specification review

The independent specification review found that dotted numeric bind hosts such
as `999.999.999.999:0` passed hostname-label validation. The parent added invalid
IPv4 cases to the existing authority test, checking decode and constructed-value
validate/encode. Valid unspecified and broadcast IPv4 addresses remain covered
alongside hostname and IPv6 cases. Before the production correction, the focused
test failed on `999.999.999.999:0` (one failed, six filtered, 0.01s).

The bind validator now parses hosts containing only digits and dots, with at
least one dot, as `std::net::Ipv4Addr` before the hostname branch. It performs no
lookup. The corrected full configuration test target passed all seven groups in
0.02s. Strict targeted Clippy passed (10.99s), as did owned rustfmt and whitespace
checks. Commands from the repository root were:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_configuration advertised_and_bind_authorities_have_distinct_port_and_path_rules -- --exact
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test control_configuration
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test control_configuration -- -D warnings
rustfmt --edition 2024 --check server/src/cluster/configuration.rs server/tests/control_configuration.rs
git diff --check -- server/src/cluster/configuration.rs server/tests/control_configuration.rs
```

Fresh independent specification re-check passed without a concrete scoped
regression. It confirmed the dotted numeric host correction and inspected valid
IPv4, hostname and IPv6 cases plus malformed decode/validate/encode assertions.
The reviewer reused the parent's recorded red/green and tooling evidence and
performed no tests, builds or edits. Independent quality review remains pending.
Separately, the parent adopted A08's future-stop reservation contract in A05 and
the shared contracts; that arithmetic is not implemented or tested by this slice.

Independent code-quality review subsequently passed without important findings.
It inspected the strict schemas, bounds, arithmetic, diagnostics, canonical
fingerprint, dependency delta and actual test assertions. It reused recorded
verification without builds, tests or probes. This completes the codec slice;
feature-enabled S3 and runtime/attachment/root-transition behavior remain outside
its evidence.
