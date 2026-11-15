# DN-R03a3c consuming the original reply context

Implemented only adopted DN-A18 assignment 3 on shared main, without delegation.
C05a released Rust/Cargo ownership before this work. R03a3a/b passed both reviews.
Independent specification review followed by quality review remains parent-owned.
Aggregate R03a3, actual admission, R04, HTTP and distributed acceptance remain open.
A20 is adopted design; its runtime implementation is deferred.

Read applicable distributed contracts, the distributed-nodes orchestration,
R03 and testing rubric, adopted A18 assignment 3 and final receiver/reply
transactions, relevant A17/A07/A15/A10, completed R03a2b1/b2/b3 and R03a3a/b
notes, and CLOSED DN-test-lints. Inspected actual verification, content,
eligibility, wire, complete-root observation and existing private trait tests.
Applied test-driven-development and unslop skills. No applicable AGENTS.md was
found in the repository or checked ancestors through the filesystem root.

## Four-file delta and preservation

Only these four files were written:

- `server/src/node/auth/verify.rs` adds the consuming method, ReplyHandoff,
  ReplyContextBinding, ReplyBody, their borrowed accessors and value-free Debug,
  plus the private test declaration. Removed obsolete item allowances from
  RequestContext, ReplyAssociation, ReplyContent, CheckedReplyContent and
  EligibleReply. The unused retained canonical reply field has its own
  documented allowance. New pending runtime items use narrow item allowances.
- `server/src/node/auth/verify/reply_handoff_tests.rs` is new, with five causal
  groups using real contexts, signatures, content and provisional eligibility.
- `server/src/node/auth.rs` adds only the crate-private reexports of ReplyBody,
  ReplyContextBinding and ReplyHandoff with a pending-consumer import allowance.
- This new delivery note.

Pre-edit copies of both existing owned files are retained outside the checkout
at `/tmp/meshdb-R03a3c.GIeP1R/{verify.rs,auth.rs}`. Both new paths were absent.
Snapshot comparison confirms the original verify.rs prefix differs only in the
five obsolete item allowances and the narrower canonical-field allowance.
Every original definition, algorithm, method body, signature and test is
preserved. auth.rs has only the appended reexport. No credentials, wire,
content/eligibility, replay/sequence, controller, payload, runtime, Cargo/lock,
plan, Python or unrelated file was edited. Shared preexisting changes remain
uncommitted. No branches, worktrees, commits or pushes were created.

## Exact API and ownership

The concrete crate-private method is:

```rust
RequestContext::into_reply_handoff(
    self,
    eligible: EligibleReply,
    content: CheckedReplyContent,
    current: &OwnershipObservation,
) -> Result<ReplyHandoff, Error>
```

It requires the exact same reply Arc in eligible/content, exact request,
expected-content and target Arcs against self, and full eligible observation
equality with current. Every mismatch returns static Error::Invalid. It does
not compare current with the older target observation retained at send.
Moving self consumes the original context on success and failure. There is no
context-returning error, retry method or extraction of an owned RequestContext.
verify_reply retains its existing borrowed API and may produce several distinct
reply Arcs before one candidate consumes the actual context.

ReplyHandoff privately retains ReplyContextBinding containing the original
RequestContext and reply Arc, the eligible observation and PendingReplyChecks,
and the original CheckedReplyContent containing its reply Arc, boxed bytes and
parsed value. All getters borrow immutable data. Its context(), binding(),
auth(), status(), bytes(), pending(), observation(), operation(), job(),
readiness(), dependency(), close(), drain(), error() and
is_unattributed_rejection() preserve the checked interpretation.

The consuming into_parts returns exactly:

```text
(ReplyContextBinding, Box<[u8]>, ReplyBody,
 OwnershipObservation, PendingReplyChecks)
```

The opaque binding retains all original invocation associations and exposes
only auth(), initial_bytes(), expected_job() and reply_auth() borrows.
expected_job retains the full original snapshot/ordinal/table/task expectation.
ReplyBody is closed over Operation(Box<RawValue>), Job(Box<ResultEnvelope>),
Readiness(ReadinessReply), Open(Dependency), Close(DependencyCloseReply),
Drain(DrainReply), Rejection(ProtocolError) and
UnattributedRejection(ProtocolError). Conversion moves the actual boxes and
bytes, without parsing, hashing, serialization, copying their allocations or
requiring unique Arc ownership. Errors remain remote data, including unknown
outcomes and unattributed classification; this creates no durable DB decision.

All new fields are private, with no Clone/Serialize/Deserialize, unchecked
constructor, mutable proof access or raw-parts reconstruction. Debug emits
only type names. There is no disk/path/spool transfer, generic callback
transaction, reply replay map, new response sequence or background work.

## Red and green evidence

Every Cargo invocation used the existing server/target and this environment:

```sh
CARGO_TARGET_DIR=server/target CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

The exact focused command, run for red and green, was:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::verify::reply_handoff_tests -- --test-threads=2
```

Two initial fixture attempts failed compilation: assert_eq required Debug on
the intentionally opaque OwnershipObservation, then a copied job JSON fixture
had an unmatched delimiter. The former also reported two imports pending use
in the remaining tests. These are compile failures, not missing-API failures
or behavioral-red evidence.

With a compiling consuming placeholder returning Err(Invalid), the focused run
exited 101 after 39.31s compilation and 0.04s tests. Three valid operation/job/
Debug setups failed at consuming unwrap. The negative association group passed,
as expected for an always-reject placeholder. The small-observation group
instead exposed a fixture error before consumption: the existing checked wire
contract rejects initialized:false. No handoff-red claim is made for that group.
Its corrected test first verifies signed initialized:false is rejected by
check_reply_content, then transfers the valid initialized:true body. This
preserves the existing decoder and its A07 semantics. False closed and idle
observations remain valid and are transferred unchanged.

After implementing the consuming checks and correcting that fixture, all five
groups passed without warnings: exit 0, 30.80s compilation, 0.06s tests,
200 filtered. No repeated full crypto/codec matrix was run.

The groups cover different reply Arcs from separate real verifications of the
same response/context; separately constructed identical-ID contexts; newer or
equal-revision divergent complete observations; and positive unrelated root
progress with the eligible observation retained. Roots are round-tripped,
validated generation-2 Control values before negative changes. Checked bodies
and eligibility succeed before association/observation rejection is attempted.

Successful transfer compares original initial/reply-byte pointers and the
operation RawValue allocation through both borrowed and consuming access.
The job group compares the ResultEnvelope box pointer, complete JobEnvelope
equality and original expected-object pointer after transfer, while preserving
pending coordinator/operation/admission/permit/job/attempt claims. Small-body
and error groups cover all remaining owned variants, false closed/idle,
heartbeat/challenge echoes and attributed/unattributed ProtocolError data.
The existing negative-trait ambiguity technique pins non-Clone/non-Serde for
RequestContext and all three new types, with value-free Debug assertions.
Field privacy and absence of extraction/raw constructors were inspected in
source; these trait tests are not a separate external compile-fail privacy test.
No test pretends to call a moved context twice. No storage fake or path include
was needed. Test signatures use the existing worker signing/verification API;
advertised body hashes use sha2 separately from content's ring SHA256.

Affected existing regressions were run sequentially with the same Cargo
environment. This exact loop passed, exit 0, for eight tests total:

```sh
for meshdb_test_filter in \
  node::auth::verify::content::tests::reply_correlates_sequence_status_sender_scope_target_and_domain \
  node::auth::verify::content::tests::jobs_keep_full_expectations_and_signed_claim_mismatches \
  node::auth::verify::content::tests::small_echoes_preserve_pending_and_idle_false \
  node::auth::verify::eligibility::tests::replies_allow_accepted_draining_work_but_recheck_context_and_current_binding \
  node::auth::verify::eligibility::tests::accepted_job_reply_and_distinct_coordinator_recheck \
  node::auth::replay::tests
do
  CARGO_TARGET_DIR=server/target CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 \
    cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features \
      --lib "$meshdb_test_filter" -- --test-threads=2 || exit $?
done
```

The five individual regressions passed in 0.02-0.04s each. All three replay
groups passed in 8.59s. The additional existing error-classification regression
uses the following exact command and the same environment:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::verify::content::tests::strict_error_envelopes_and_unattributed_operation_rejections -- --test-threads=2
```

Final error-regression, strict Clippy and preservation results are recorded below.

## Caller obligations and limits

This helper establishes memory association and consumption only. The reply
owner must freshly load/validate the root, reconcile complete observations
monotonically and verify the actual live sending boot. Under its actual short
operation/context admission mutex it must require the exact retained accepted
operation/digest or coordinator/dependency/admission/permit/job/attempt, match
live challenge/permit/drain state, and synchronously reserve continuation or
result-delivery ownership before consuming the context. No fallible acquisition
may follow consumption. Refusal drops the attempt; cancellation or supervisor
setup failure must retain and settle the actual reserved owner.

The receiver transaction remains separate. After outside-lock fresh GET,
validation, encoding, signature/hash/parse, its one admission mutex must combine
root ordering and local boot with live barrier/permit/capacity/queue/registry
checks, complete all fallible reservations, consume replay, then infallibly
install accepted ownership before unlock. Queue-start/result rechecks, bounded
prelude supervision, same-reader checked CONTROL transfer, real EOF/errors/
trailers, remaining row/file validation and delivery/publication/cleanup still
need runtime integration. A07's fresh-GET revocation race, trusted-member limits,
C03/C06 initialization/idle evidence and A10 guard/generation/owner/intent/exact
CAS token duties remain applicable. A20 is not implemented by this assignment.

One actual boot sender owner must serialize the shared sequence counter across
all routes/targets, forbid duplicate outgoing sender/target/sequence invocations
and preserve replacement-boot rules. Distinct contexts are distinct memory
invocations even with identical IDs. Arc identity cannot cryptographically
repair deliberate sequence reuse across separately reconstructed contexts.
No claim of final admission or aggregate R03a3 completion is made.

## Final verification and release

The additional strict error-envelope regression passed, exit 0, one test,
204 filtered, 0.03s tests after 32.64s compilation. The only intervening source
change was documentation of the one boot sequence owner's obligation.

Final strict Clippy passed without diagnostics, exit 0, in 29.61s:

```sh
CARGO_TARGET_DIR=server/target CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
```

The CLOSED test-target lint backlog remains closed. No blanket/module-wide
allowance was added and no unowned lint correction was needed.

Owned formatting passed, exit 0:

```sh
rustfmt --check --edition 2024 --config skip_children=true server/src/node/auth/verify.rs server/src/node/auth.rs server/src/node/auth/verify/reply_handoff_tests.rs
```

A direct snapshot preservation script reconstructs the five allowed obsolete
attribute removals and canonical-field attribute, then asserts exact equality
of the entire original verify.rs prefix. It also asserts exact auth.rs equality
with only the appended reexport. Both passed. Direct trailing-whitespace and
single-final-newline checks passed for all four owned paths, including both new
untracked files. Scoped git diff --check also passed, with the direct checks
covering files omitted by git's tracked diff. Disk headroom stayed about 2.2 GiB.

These are component checks with synthetic validated roots and memory ownership,
not a fresh storage observation, runtime reservation or network guarantee.
No network/cloud, dependency download/fetch, secret/.env/profile read, target
deletion or unrelated cleanup occurred. No background task was started.

Cargo/build and Rust implementation ownership is explicitly RELEASED to parent.
Independent specification review, then independent quality review, remains
parent-owned. No aggregate R03a3 completion is claimed.
