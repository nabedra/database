# DN-C01c authenticated HTTP storage and typed metadata

Status: implemented and tested; independent specification re-check and
code-quality review passed after the authority validation correction.

## Scope and API

Added `storage::HttpStore`, also available as `storage::http::HttpStore`.
The exact constructor is:

```rust
pub fn new(
    base_url: &str,
    namespace: Namespace,
    bearer_token: &str,
    ca_pem: Option<&[u8]>,
) -> Result<HttpStore, StorageError>
```

Use `Storage::new(HttpStore::new(base, ns.clone(), token, ca)?, ns)`.
Construction validates locally without network access. The adapter implements
ObjectStore with already namespace-qualified keys. It checks exact namespace
containment, the shared ObjectKey allowlist, ASCII unreserved path components,
and the helper's 4096-byte qualified-key bound before network access. Base URLs
must use HTTP or HTTPS, with no credentials, query, fragment, percent encoding,
backslash, empty interior path component or dot component. A safe optional base
path is preserved before `/v1/objects/<qualified-key>` without URL encoding.
Bearer tokens must be nonempty printable ASCII without spaces. Header injection
and malformed Match tokens fail locally. HttpStore has no Debug implementation;
the Authorization HeaderValue is marked sensitive. Errors and read I/O messages
are fixed sanitized text, with no underlying diagnostics or configuration.

Cached Hyper 1.12.0, Hyper-util 0.1.21 and Hyper-rustls 0.27.10 provide HTTP
parsing and verified TLS. There is no bespoke production HTTP parser. The
connector's TCP connect timeout is 5 seconds; a 30-second total request deadline
also covers DNS/TLS and GET body consumption. PUT returns after response headers,
since successful 204 responses are bodyless. No proxy middleware, redirect
handling, environment credential loading or HTTP retry middleware is installed.
Hyper's canceled-request retry is explicitly disabled and idle pooling is zero.
PUT is submitted once, including on transport failures and redirects.

Explicit PEM material replaces the trust roots and must contain at least one
parseable X.509 CA certificate. Without explicit material, HTTPS uses the first
readable conventional system PEM bundle at `/etc/ssl/cert.pem`,
`/etc/ssl/certs/ca-certificates.crt`, or `/etc/pki/tls/certs/ca-bundle.crt`.
An unavailable/invalid bundle is Configuration. Native keychains and certificate
or proxy environment overrides are deliberately not consulted. Deployments
without these files must supply PEM material. Rustls's default certificate and
server-name verifier remains enabled with an explicit AWS-LC crypto provider.
AWS-LC is a local TLS crypto library, not an AWS storage client or cloud call.

## Reads, writes and keys

GET 200 requires exactly one valid strong opaque ETag of at most 1024 wire bytes.
The enclosing quotes remain part of the token. RFC obs-text is represented in
String tokens by a reversible Latin-1 byte mapping; callers must pass the returned
String unchanged to Match. No stripping, generation parsing, or hash guessing
occurs. Incoming body chunks feed Tokio StreamReader and a deadline reader.
Storage's existing byte-budget probe is unchanged; bodies are never eagerly
collected by the adapter. Missing Content-Length is permitted using normal HTTP
framing. Truncated length/chunk framing and body failures yield sanitized I/O
errors, which Storage maps to Unavailable. As with HTTP generally, a clean
connection close is the delimiter for a close-delimited response.

PUT accepts at most 80 MiB, sends the exact Content-Length and exactly one
If-None-Match `*` or valid strong If-Match token. Success requires 204, exactly
one valid ETag, no Transfer-Encoding, and either absent Content-Length or a single
Content-Length `0`. Positive, duplicate or malformed framing is not accepted as
success. Hyper also rejects invalid HTTP response syntax.

Error mapping is deliberately conservative:

- Local malformed constructor/token configuration: Configuration.
- Local qualified-key/namespace/immutable-Match violation: InvalidPath.
- Local body above 80 MiB: LimitExceeded, before sending.
- GET 404: NotFound; other GET status/protocol/transport failures: Unavailable.
- PUT 412: Conflict.
- PUT explicit helper rejection 400/401/403/411/413/414/417: Unavailable.
- All other PUT statuses, including redirects, unexpected success statuses,
  5xx, transport loss/timeouts, invalid ETag or malformed acknowledgment:
  Ambiguous. Callers must reconcile rather than blindly resend.

ObjectKey now has `decision(Uuid)`, `control_decision(Uuid)`, `control()`, and
`heartbeat(Uuid)` constructors. parse_relative admits only their canonical
lowercase UUID paths plus existing catalog/snapshot/partition paths. Storage
and HttpStore allow Match only on catalog, cluster/control.json and heartbeat
keys. Decisions and table objects remain Create-only. S3 implementation is
unchanged and continues to use the shared parser for key guarding. Its existing
catalog-only Match check and raw-S3-ETag non-ABA limitation are not solved or
claimed conformant for future distributed metadata operation by this slice.

## Evidence inspected

Read plans/distributed-contracts.md as authoritative, storage.rs, storage/s3.rs,
tests/storage.rs, DN-C01b, scripts/local_store.py and C01a notes/contracts.
No applicable AGENTS.md was found in ancestor paths or below server.
Applied test-driven-development and unslop skills. Inspected installed registry
sources for Hyper-util retry_canceled_requests, HttpConnector connect timeout,
Hyper-rustls with_tls_config/wrap_connector, and rustls-pki-types PemObject.
No online API documentation, cloud calls, .env files, delegates, branches,
worktrees, commits or pushes were used. No runtime/catalog-v3/node/control work
was added. Substantial pre-existing changes remain intact.

## Commands run and results

Every Cargo invocation used the existing server/target and this exact prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Commands were sequential, from repository root, with no default-feature AWS
build or broad suite:

1. `cargo test --manifest-path server/Cargo.toml --offline --locked --no-default-features --test http_store canonical_metadata_allowlist`
   initially FAILED, 0 passed / 1 failed, because the first required decision
   path returned InvalidPath. This was a real executed behavior failure before
   implementation.
2. Same test target without the name filter, before implementation, failed to
   compile because HttpStore and new typed constructors did not exist.
3. `cargo test --manifest-path server/Cargo.toml --offline --no-default-features --test http_store`
   resolved exclusively cached dependencies and updated only the root package's
   dependency list in Cargo.lock. It exposed an async closure lifetime error in
   the new test. The next locked invocation repeated that compile error; fixing
   the closure's captures produced 4 passing tests.
4. New unsafe-basepath and TLS coverage produced 4 passes / 1 expected failure
   because a `//` base path was accepted. Added obs-text coverage then ran
   `cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test http_store incomplete_and_malformed_responses_never_claim_success_or_retry`
   and FAILED on rejection of a legal opaque obs-text ETag. Both production
   corrections followed these observed failures.
5. `cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test http_store --test storage`
   PASS, 5 HTTP tests and 8 storage tests. HTTP tests finished in 1.26s,
   storage tests in 0.20s.
6. `cargo check --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib`
   PASS.
7. `cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test http_store --test storage -- -D warnings`
   initially found one needless borrow in the test. After that semantics-neutral
   correction, the same command PASS. Tests were not redundantly repeated.
8. Owned-file rustfmt applied formatting; the final `rustfmt --edition 2024 --config skip_children=true --check server/src/storage.rs server/src/storage/http.rs server/tests/http_store.rs`
   PASS. `git diff --check` for all owned files and a direct whitespace/syntax
   check of the new files also PASS.

Actual TCP evidence includes a Python 3.11 local_store child with fresh temporary
SQLite state, env-injected disposable token and port-zero announcement. The
helper bounds startup waiting, suppresses child stderr, and kills/waits owned
children and removes temporary state on Drop. Tests cover create/read, identical
bytes advancing ETags, stale rejection, two same-token writers released by a
barrier with exactly one winner, restart preserving bytes/version, read cap,
mutable control/heartbeat keys, immutable decisions, and auth/path rejection.
Restart deliberately kills and waits the service rather than assuming a flush
on graceful exit. No helper full suite was rerun.

A tiny loopback raw TCP fixture consumes the complete request and counts any
additional connection. It proves truncated GET rejection, missing/weak/duplicate
ETag rejection, close-delimited GET, opaque comma/obs-text tags, lost PUT reply,
malformed success, 5xx and redirect outcomes, all with exactly one request.
A separate owned Python TLS fixture reuses scripts/test_tls_material.py and
proves explicit CA success and rejection without the CA. Hostname verification
is preserved by library configuration; a separate negative hostname test and
wall-clock timeout fault tests were not run. No claim is made about cloud,
power-loss durability or full cluster integration.

## Specification correction: explicit authority ports

The independent specification review found that the HTTP URI parser accepts
malformed explicit ports and returns no parsed port, causing the connector to
select its default. The coordinator extended the existing constructor scenario
with nondecimal, out-of-range and empty ports for hostnames and bracketed IPv6,
plus an empty host. Valid default/explicit hostname and IPv6 endpoints remain
accepted. The focused scenario failed before the production correction.

Construction now rejects an empty host and validates the authority suffix after
the parsed host (including IPv6 brackets). Any explicit port must be nonempty
ASCII decimal fitting u16. Invalid authorities return local Configuration before
client construction or networking, without selecting a default port.

All Cargo commands used the documented resource flags and ran sequentially:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test http_store constructor_rejects_unsafe_configuration_locally
Before correction: 0 passed, 1 failed, exit 101.
After correction: 1 passed, 0 failed, exit 0 (0.00s).

cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --test http_store -- -D warnings
Passed, no warnings (5.99s).

rustfmt --edition 2024 --config skip_children=true --check server/src/storage/http.rs server/tests/http_store.rs
Passed.
```

Scoped git whitespace checks passed. The other four HTTP scenarios and eight
storage tests retain their earlier evidence; no unrelated suite was repeated.
The coordinator changed only the constructor guard, its existing test and this
evidence for the correction. Fresh specification re-check is required.
