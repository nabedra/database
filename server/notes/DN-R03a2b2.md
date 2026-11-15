# DN-R03a2b2 exact content and retained reply contexts

Implemented adopted DN-A17 mini 2 only. Parent specification and quality reviews
remain required. Mini 3, R03a3 and runtime integration remain open.

Read shared distributed contracts, R03 and its testing rubric, adopted A17 in
full, relevant A07/A15/A05/A10 contracts, reviewed R03a1/R03a2a/R03a2b1 notes,
and actual wire, verification, credentials, job, protocol, response, decision,
member/state and OwnershipObservation implementations. No applicable AGENTS.md
was found in checked ancestors or scoped repository paths. Applied the
test-driven-development and unslop skills. No delegation occurred.

## Files and concrete APIs

New `src/node/auth/verify/content.rs` implements these crate-private synchronous
Result-returning boundaries, with private child tests in `content/tests.rs`:

- `check_request_content(&SignedRequest, Vec<u8>) -> Result<CheckedContent, Error>`
- `RequestContext::for_send(Auth, Vec<u8>, &Control) -> Result<RequestContext, Error>`
- `verify_reply(&str, &str, u16, &RequestContext) -> Result<SignedReply, Error>`
- `check_reply_content(&SignedReply, Vec<u8>) -> Result<CheckedReplyContent, Error>`

Additive definitions in `verify.rs` own the private content/context types and
closed enums, response-only fixed-domain verifier, child module and concrete
reexports. The earlier request/enrollment implementation and tests are unchanged.
`auth.rs` adds narrow private reexports and separates consumed wire imports.
`wire.rs` adds only Auth.route, the listed small-body identity/boolean/sequence
getters and EnrollmentReply identity/registration/checked API-key getters, and
removes consumed Auth/ReplyAuth impl allowances. Existing wire validator,
decoder and encoder bodies and tests are unchanged. Mixed pending wire macros
retain their existing item allowances. This note is the sixth owned file.

All proof/context fields are private and non-Clone/non-Serialize/non-Deserialize.
There are no mutators, unchecked constructors or detached-parts accessors.
Debug prints only the concrete proof/context type name. Auth errors remain
static Invalid/Limit; remote error messages are parsed DATA, never diagnostic
sources. Borrowed typed content accessors expose operation/job/small/error claims.
RequestContext exposes only auth() and initial_bytes() for unchanged sending;
SignedReply exposes borrowed auth(). No new public-library facade is introduced.

Request checking bounds before SHA256 and parsing. Operations require both
control_sha256 and request_digest to equal the original exact-byte hash, then
parse_request and operation UUID agreement. Jobs use parse_job and compare
cluster/operation/job/attempt/digest; admission and dependency remain in Auth.
Small bodies use the existing checked wire decoders. Original bytes, whitespace
and numeric spellings are retained without reserialization.

for_send consumes checked Auth and original outgoing bytes through that same
private checker; it does not manufacture incoming signature evidence. It
validates/observes the complete supplied generation-2 root, matches the exact
target node/incarnation/session and retains its durable verifier, rejecting a
missing, replaced, retired, lost or stopped binding. This observes supplied
state and does not certify freshness or current route authority.

Layout: RequestContext owns one Arc<RequestAssociation> and original boxed
bytes. Complete immutable ExpectedContent and TargetBinding have separate
private Arcs shared with replies, avoiding repeated copies of a full Request or
JobEnvelope. Large operation/job enum payloads are boxed. Each SignedReply owns
one distinct Arc<ReplyAssociation>, retaining the SAME request Arc, expected
content/target Arcs, full ReplyAuth and canonical reply bytes. CheckedContent
and CheckedReplyContent retain their respective exact request/reply Arcs.
These memory-only Arc clones expose no Clone proof or detachable proof pieces.
Mini 3 can compare invocation pointer identity, even for identical UUIDs/bytes.

Reply verification checks canonical ReplyAuth and Ed25519 over exactly
`b"meshdb.http-response.v1\0" || canonical`, with the 4096-byte allocation bound.
It compares complete original sender identity/key/process and nullable fields,
cluster, positive request sequence, scope, actual/signed status and fixed
internally selected statuses. All six routes permit success 200 or the existing
protocol error statuses 400/401/404/409/413/429/500/503. Target identity and
process/incarnation must match, and recomputed worker_verifier must equal the
retained durable commitment. This requires no public-key discovery protocol.

Reply content hashes exact bytes before parsing, after applying the route cap.
Job success uses parse_result against the entire retained JobEnvelope. Small
successes compare challenge/permit/drain echoes and preserve closed:false and
idle:false. Existing initialized/barrier/positive-heartbeat validation remains
with wire. Operation success uses a new narrow strict object/EOF reader for
unique request_id/data fields, matching the retained UUID and keeping data as
exact opaque Box<RawValue>. It does not decode ExecuteResult or Decision, nor
validate CRUD result schemas. A lexical nesting preflight preserves serde_json's
default bound, including inside opaque RawValue data, without converting it.

Non-200 replies use a strict object/EOF error reader with required nullable
request_id, strict nested object and string-only existing ErrorCode/Outcome.
Unknown/duplicate fields, object enum forms and status/code disagreement reject.
A supplied UUID cannot conflict with a known retained operation UUID, including
job/dependency transport errors. Null operation IDs yield the distinct private
UnattributedRejection variant, retaining signed status/error/context and never
attributing terminal outcome to the operation. Both Unknown and NotApplied,
including response-encoder fallbacks, are preserved without Decision's terminal
whitelist. HTTP 200 JobResult::Error still follows framed parse_result.

Limits: operation requests/success/errors 8 MiB; job request/result CONTROL
16 MiB; small requests/successes and non-operation transport errors 16 KiB.
Operation envelopes retain the default JSON nesting bound; small decoders retain
depth 16 and job parsers retain their own bounds. Bounds precede hash/parse and
are source/allocation caps, not an RSS measurement.

## Tests and command evidence

Every Cargo invocation reused server/target with this exact environment prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Every invocation also supplied `--manifest-path server/Cargo.toml --locked
--offline --no-default-features`. No dependency/manifest/lock changes, fetch,
default/AWS build, target deletion or external network/cloud calls occurred.

Initial behavioral red: opaque_success_keeps_exact_data_and_checks_uuid ran
against a compiling rejecting placeholder. It failed at unwrap with Invalid,
exit 101, after 53.36s build. This was behavioral red, not compile-only failure.
The first implementation passed six groups after 47.47s build. Four later
green-only groups brought this to ten passing groups after 28.72s build and
2.30s tests. The numeric-spelling/encoder-fallback addition then passed eleven
groups after 23.35s build and 1.87s tests. No separate red is claimed for those
later additions.

A second focused behavioral red added
transport_error_rejects_a_mismatched_known_operation_uuid, with a valid UUID
positive control. Before correction it accepted the different known UUID and
failed its assertion, exit 101, after 23.76s build. The correction compares any
supplied error UUID with the retained Auth operation UUID before classification.
No compile-only failures occurred in these runs.

Fixtures use real checked wire records, actual parsers and a private external
synthetic generation-2 Control through real encode/decode. Synthetic signing
uses ring Ed25519KeyPair seeds only in tests and independently assembled
canonical wire text. Advertised hashes use independent sha2 SHA256; production
checks use ring SHA256. Earlier reviewed crypto vectors are reused through the
unchanged verify regression suite. No Fake, disk fixture, threads/tasks, real
credentials or .env material is needed.

Coverage includes exact hash versus consistently signed parsed-claim mismatch,
full job expectations, same-UUID invocation separation, complete sender/target
binding, distinct sequences for distinct operation/admission/attempt contexts,
status/scope/domain mismatch, small echoes and false states, raw opaque success
data, UUID and strict error/outcome/status handling, unattributed rejection,
encoder fallbacks, malformed/duplicate/trailing/object-enum envelopes, recursion
and cap+1 with valid-at-cap controls, and negative proof trait/redaction guards.
This is focused boundary evidence, not a repeated CRUD/job/codec matrix.

Final focused command passed all twelve groups after 28.01s build and 2.33s
tests. Existing verify regressions ran once and passed eight groups in 0.01s
after 1.33s preparation; existing wire regressions ran once and passed eight
groups in 0.01s after 1.19s preparation. These protect reused fixed-domain
vectors, canonical codecs and the no-Deserialize boundary. Credentials were
not edited; their vector/primitive matrix was not repeated.

Commands, each with the exact environment prefix above:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::verify::content::tests
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::verify::content::tests::transport_error_rejects_a_mismatched_known_operation_uuid -- --exact
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::verify::tests
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::wire::tests
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
```

No broader test suite or all-target/test-target lint was run; the parent's six
unrelated test-target lint diagnostics remain parent cleanup. Scoped rustfmt
with `--edition 2024 --config skip_children=true` and its `--check` passed on
all five owned Rust files. Scoped git whitespace and direct checks including
untracked files cover all six owned files. Final lint result follows below.

## Later caller obligations

The sender must allocate a never-reused positive sequence across ALL routes for
the same complete sender/key and target session, with checked exhaustion.
ReplyAuth contains sequence and scope, NOT operation/admission/job/attempt IDs.
The complete retained context and that temporal sequence obligation bind those
IDs. No test here establishes the future allocator or replay property.

Mini 3 owns provisional current-root authority, pinned API checks and association
combination. R03a3/R04 own fresh GET, monotonic complete-root observations, current
local permit/barrier/capacity checks and atomic replay consumption/reservation.
Hash/parser success certifies neither terminal database outcome nor framed
payload termination/completeness, runtime delivery, readiness or idle authority.
C02 and engine/reducer own publication, terminality and actual success semantics.

A15's supervised prelude must retain bounded admission through submission,
accepted work, cancellation and settlement; hash/parse off the short admission
lock; then transfer checked CONTROL once with the SAME advanced FrameReader.
DATA/PART_META/rows/prepared files, STREAM_END, trailers and actual HTTP EOF/body
errors remain later payload/transport obligations. No hidden retry is introduced.
EnrollmentReply remains unsigned; its new getters grant no trusted-channel API
key pinning authority. Later enrollment must associate every identity through
verified TLS or the explicitly trusted local channel before pinning.

Shared uncommitted/untracked work is preserved. No protocol/job/engine/decision,
cluster/member/state, credential implementation or existing tests, Cargo,
Python, rubric or adopted design file was edited. No branch/worktree/commit/push,
real material, cloud, provider or network operation occurred. No aggregate R03
or distributed acceptance completion is claimed.

Final strict production-library Clippy passed on its first run without
diagnostics in 12.93s. Formatting/whitespace checks passed, including direct
checks of the untracked files and final note. Disk headroom remained 2.4 GiB.
No resource or interface blocker remains for this slice.

Cargo/build and Rust implementation ownership is RELEASED to the parent for
separate specification review followed by quality review. No mini-3 or replay
implementation was started.
