# DN-R03a2b3 provisional membership and route eligibility

Implemented adopted DN-A17 mini 3 only. Independent specification then quality
review remains parent-owned. R03a3/R04 final admission and runtime integration,
and the aggregate authentication/distributed acceptance work, remain open.

Read the distributed contracts, relevant R03/C03/R04 and testing rubric, adopted
A17 in full, A07 including its drain corrections, A15 and relevant A05/A10/A16,
and R03a1/R03a2a/R03a2b1/b2 delivery notes. Inspected actual checked wire records,
content/context associations, worker keys, Control/member validation and complete
OwnershipObservation. Applied test-driven-development and unslop skills. No
applicable AGENTS.md was found in ancestors or scoped repository paths. A broad
read-only parent-directory discovery returned sibling-project guidance only.
No delegation occurred.

## Files and API

Five files belong to this slice:

- New `src/node/auth/verify/eligibility.rs` implements synchronous
  `check_request_authority(&SignedRequest, &Control, &ReceiverIdentity, &[u8;32])`
  returning `Result<EligibleRequest, auth::Error>`, and
  `check_reply_authority(&SignedReply, &Control, &RequestContext)` returning
  `Result<EligibleReply, auth::Error>`.
- New private `eligibility/tests.rs` contains seven focused groups.
- Additive definitions in `verify.rs` own ReceiverIdentity, EligibleRequest,
  EligibleReply, DependencyBinding, PendingLocalChecks and PendingReplyChecks,
  plus the private child declaration and concrete reexports.
- `auth.rs` adds narrow crate-private reexports.
- This delivery note.

All proof fields remain private. No Clone, Serialize, Deserialize, mutator,
unchecked proof constructor, detached mutable record or accepted-admission API
was added. Debug emits type names only. Errors remain static Invalid/Limit
categories. No previous test, content parser, credential, wire, manifest/lock,
cluster/controller/member/store, payload/frame/spool, runtime or Python file was
edited. Existing read-only getters sufficed; no fixture visibility change or
public test facade was needed.

ReceiverIdentity::new captures typed cluster/node/incarnation/session UUIDs and
the supplied local WorkerSigningKey public key. It proves neither initialization
nor enrollment. same_boot compares every captured field including the key.
EligibleRequest retains the exact request Arc, one complete root observation,
a private copy of that exact local binding, and typed pending requirements.
EligibleReply retains the exact reply Arc, its complete original association and
expectation/target Arcs, the current complete observation, and typed pending
reply requirements. matches_content compares Arc identity, never UUID equality.
Borrowed auth(), observation(), pending() and request local() accessors grant
no admission; DependencyBinding exposes borrowed coordinator and copied IDs/
fixed digest bytes. Content's existing typed accessors supply parsed claims.

Both authority functions validate and encode the supplied complete root through
OwnershipObservation::new. Worker checks require exact current node/incarnation/
session, no retirement or lost/stopped state, and recomputed worker_verifier.
Request targets must also equal the captured local boot and key. API sender
keys must equal the separately supplied trusted enrollment pin. Replica process
UUIDs may differ while sharing that key. Reply checks first require the original
request, expectation and target Arc associations, then recheck current target
key/identity/phase and the original worker sender's current binding. Retained API
sender/key association comes from the original context, without a controller
ownership restriction on ordinary API data requests.

## Pending requirements and phase rules

PendingLocalChecks is closed over these variants:

- NewOperation retains operation UUID and fixed request digest. The final owner
  must require an open barrier and operation capacity.
- Probe requires the same initialized live boot, endpoint/storage evidence and
  exact challenge read from the matching CheckedContent.
- Open retains coordinator identity/key, operation/admission UUIDs and digest.
  CheckedContent supplies permit_id. Final admission requires an open barrier,
  a live retained operation and either an exact existing OPEN retry or room.
  A closing permit cannot reopen, even for an old operation.
- Job retains that complete binding plus permit/job/attempt UUIDs. Final
  admission requires the exact pre-barrier open permit and partition capacity;
  it never acquires an operation slot. UUIDs and time do not establish ordering.
- Close retains the complete binding, with permit_id supplied by checked content.
  Match an existing retained binding if present; absence can only be observed
  as closed:true and proves no previous execution or remote completion.
- Drain retains the durable drain UUID. The final owner must compare BOTH the
  checked request drain ID and retained current local drain/barrier identity.

The request proof itself supplies root/local binding and exact advertised
content hash for all variants. No fallible parsing/hashing is needed under the
future final lock. Small request IDs unavailable in the signed header stay
explicit content-combination obligations.

Operations/open targets require verified/ready. Readiness permits exactly
starting/registered or ready/verified, never draining. Jobs require BOTH workers
verified and ready/draining. A draining verified coordinator can finish retained
operation dependencies, including opens on ready targets whose local barrier
still permits them; a drained target refuses opens. Close permits current
ready/draining bindings, including registered draining recovery. Drain requires
registered/verified durable draining with a retained drain record. Registered
draining recovery never grants job execution.

PendingReplyChecks has AcceptedOperation and AcceptedJob variants retaining
operation/digest or full dependency/job/attempt binding, plus Probe/Open/Close/
Drain retaining exact parsed context challenge/permit/drain IDs and dependency
bindings where relevant. Accepted operation/result replies permit a verified
draining target but still require retained accepted-work/context evidence.
Small replies retain live context requirements, not operation admission.
Drain context IDs must already equal the current durable drain ID. Neither
readiness eligibility nor a signed idle observation creates C03 ReadinessInput
authority or confirmed_idle.

## Validation and chronology

The initial eligibility command failed compilation because new proof/API names
were absent. After adding the private shapes and Err(Invalid) placeholders,
the single valid operation test failed behaviorally at authority unwrap. Its
signature, parsed content and real generation-2 round-tripped root had passed.
That run took 27.92s compilation and reported one unused-import warning in the
placeholder. This is the observed behavioral red.

Further tests were written before the concrete authority implementation. They
were not individually observed failing against the placeholders. The first
implementation run failed compilation on a missing return-borrow lifetime in
current(); tying it explicitly to the root fixed it. The next run passed four
groups, 157 filtered, 0.04s after 24.10s compilation. Additional final assertions
and three groups were added afterward and are green-only evidence. The initial
job phase test used an authority-only signed `{}` body; before final validation
it was replaced by a valid complete JobEnvelope checked by the real parser.
The final focused run passed seven groups, 157 filtered, 0.05s after 27.98s
compilation. Borrowed auth accessors were added afterward and compiled in the
following regressions and strict library Clippy; they changed no authority
behavior.

Tests use memory-only generated WorkerSigningKeys and independent synthetic
ring API/alternate keys, real signatures/content, and Control encode/decode with
generation-2 configuration. Every altered root is encoded/decoded successfully
before the rejection assertion. No storage Fake is involved. Coverage includes
session/key/incarnation/retirement/lost/stopped changes; local identity and key
mismatches; an otherwise valid signed API claim under a different synthetic
key; shared-key API replicas; readiness starting versus draining; operation
request rejection versus accepted reply completion during verified draining;
verified draining jobs versus registered recovery; distinct coordinator and
target bindings; close/drain recovery; durable drain IDs versus checked content
and reply contexts; exact retained IDs/digests; same-UUID independent request/
reply invocations; wrong RequestContext; complete-root differences at equal
revision/change UUID; and negative trait/redaction guards.

These assert provisional requirements, not real permit/capacity acceptance.
There are no bool simulations of replay/barrier races, no timing claims and no
duplicate full wire/signature/content matrix.

Every Cargo command used the existing server/target, with this exact prefix:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Commands, each with that prefix:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::verify::eligibility::tests
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::verify::content::tests
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::verify::tests
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib -- -D warnings
```

Eligibility ran five times as described. Each affected regression ran once:
content passed 12 groups, 152 filtered, 2.10s; original verification passed eight,
156 filtered, 0.02s. They protect the shared association/content definitions and
signature proof integration. Strict production-library Clippy passed first run
without diagnostics in 11.68s. No broad suite or test-target Clippy ran; the six
unrelated broader test lints remain parent-owned.

Scoped rustfmt formatting and final check used:

```sh
rustfmt --edition 2024 --config skip_children=true server/src/node/auth.rs server/src/node/auth/verify.rs server/src/node/auth/verify/eligibility.rs server/src/node/auth/verify/eligibility/tests.rs
rustfmt --check --edition 2024 --config skip_children=true server/src/node/auth.rs server/src/node/auth/verify.rs server/src/node/auth/verify/eligibility.rs server/src/node/auth/verify/eligibility/tests.rs
git diff --check -- server/src/node/auth.rs server/src/node/auth/verify.rs server/src/node/auth/verify/eligibility.rs server/src/node/auth/verify/eligibility/tests.rs server/notes/DN-R03a2b3.md
```

Direct byte checks also cover all five files, including untracked files, for
final newlines and trailing whitespace. Starting and ending disk headroom was
approximately 2.4 GiB. Shared uncommitted/untracked work was preserved. No target
cleanup, dependency download, default/AWS build, network/cloud or secret/.env
access, branch/worktree/commit/push occurred.

## Final caller obligations

This is eligibility against a caller-supplied observation. It certifies neither
freshness nor current runtime liveness. Fresh GET and root validation/encoding
remain bounded coordinator work outside short locks. Final R03a3/R04 acceptance
must compare the complete observation with the monotonic root high-water,
reject superseded/equal-revision-divergent observations, check unchanged local
boot and live permit/barrier/capacity/context conditions, then atomically consume
replay and reserve ownership only after all fallible checks succeed. Queue-start
and result acceptance need current checks too. C03/C06 effects additionally
retain A10 ownership/generation/intent/CAS duties. Payload, EOF, publication,
supervision and cancellation owners remain unchanged.

Cargo/build and Rust implementation ownership is RELEASED to the parent for
independent specification then quality review. No aggregate auth/runtime or
distributed acceptance completion is claimed.

## Parent correction: independent coordinator key

Quality review found that the distinct-coordinator fixture changed identities
but reused the target signing key. The parent changed only
`eligibility/tests.rs::accepted_job_reply_and_distinct_coordinator_recheck`:
it creates a separate coordinator key, asserts it differs from the target,
commits its verifier, places its public key in the coordinator sender and signs
the job/open requests with it. Replies continue to use the original target key.
Pending-binding assertions check the coordinator key, and the original
registration/verifier/lost-state mutations remain unchanged.

This is a test-only correction; no production defect or behavioral-red result
is claimed. Under the same resource environment and locked/offline/no-default
flags documented above, the parent ran:

```sh
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::verify::eligibility::tests::accepted_job_reply_and_distinct_coordinator_recheck -- --exact
```

The focused test passed: one passed, 171 filtered, 0.04s after 22.89s compilation.
Owned rustfmt formatting/check and direct whitespace/final-newline checks passed.
Earlier seven-group and production-library Clippy evidence is reused; no broad
suite or unchanged production lint was repeated. Fresh quality re-check remains
parent-owned. Cargo/Rust ownership is released after the focused check.
