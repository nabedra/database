# DN-A17 remaining R03a2b verification handoff

Design only, adopted by the parent after independent reconciliation passed.
Three sequential assignments refine adopted [A15](DN-A15.md),
[A07 including final corrections](DN-A07.md), and the
[contracts](../../plans/distributed-contracts.md). Preserve the
[R03/C03/R04 and testing rubric](../../plans/distributed-nodes.md),
[A05](DN-A05.md), [A10](DN-A10.md), and [A16](DN-A16.md).
These assignments implement private pure helpers, not a new protocol, enrollment
transition, HTTP adapter, supervisor, permit registry or replay implementation.
C08c2 attachment is implemented and independently reviewed. The parent assigns
exclusive Rust/Cargo ownership separately for each mini. The reviewed direct
ring 0.17.14 dependency is available; there is no dependency prerequisite.

## Reviewed starting point

`auth.rs` privately declares wire/credentials and reexports checked records.
`wire::Auth::decode(header, &ActualRoute)` already checks canonical equality,
route, role, required claims and exact dependency header. `ActualRoute::new`
takes actual method/path/dependency text. `ReplyAuth::decode` validates canonical
bytes, worker Sender, complete Sender receiver and status 100..599. It does not
associate a request. `Enrollment::decode`, `unsigned_bytes`, `public_key`, and
`proof` are checked byte/key entry points. Small records have checked `decode`
and bounded `encode`. Record fields are private. The reviewed R03a1 correction
keeps unchecked DecodeRecord and nested decoding inside wire. Never implement
Deserialize on the exposed records or move that trait out of wire.

`WorkerSigningKey::{public_key,sign_enrollment,sign_request,sign_response}`,
`ApiSigningKey::{derive,public_key,sign_request}`, and `worker_verifier` already
exist. Reuse them and ring's Ed25519 verification/SHA256. Do not add a generic
crypto engine or arbitrary-domain interface. Existing credential tests pin the
crypto expectations; the new tests exercise verification and associations.

`JobEnvelope` contains cluster/operation/job/attempt/request_digest, snapshot,
ordinal, table and task. It has no admission_id or dependency_id. `parse_job`
and `parse_result(bytes, expected)` validate CONTROL, not the remaining payload.
`protocol::parse_request` validates bounded public request syntax and operation
inputs, not table resolution or database effects. `api/response.rs` provides
bounded encoders only. `storage/decision.rs::Decision::validate` checks terminal
mutation envelopes and deliberately leaves success data to its encoder; it is
not a general operation-response decoder. `ExecuteResult` is Serialize-only.

Use real `Control::{decode,validate,encode}` and generation-2 Config, including
worker_semantic_fingerprint. Root public version stays 1. Session remains
`{id,verifier}`. `OwnershipObservation::new(&Control)` already validates/encodes
and retains complete-root identity with equality, without certifying freshness.
Neither member validation nor controller observations grant worker admission.

## Common private ownership contract

Keep the proof definitions in new `auth/verify.rs`; content and eligibility are
private children of verify. Their code can construct the parent's private fields.
Only concrete functions/types needed by callers are reexported through auth.rs.
All proof/context fields are private; no Clone, Deserialize, Serialize, public
mutator, arbitrary proof constructor, or unchecked into-parts API. Debug/errors
are value-free. Ordinary checked wire records may retain their existing Clone;
cloning a DTO creates no proof.

Use a private `Arc<RequestAssociation>` allocated once per verify_request or
send-context construction. Its fields are `auth: Auth` and
`canonical: Box<[u8]>`. This allocation is an invocation identity as well as the
complete immutable claims. Internal Arc clones do not expose a Clone proof.
SignedRequest contains only that Arc. A reply has a distinct private
`Arc<ReplyAssociation>` containing `request: Arc<RequestAssociation>`,
`reply: ReplyAuth`, canonical reply bytes, and the retained expected reply
content/target from RequestContext. SignedReply contains this Arc. Content and
eligibility retain the corresponding exact Arc. Future R03a3 compares pointer
identity as well as its receiver/root checks, never operation/job UUID alone.
Repeated verification of identical Auth bytes creates distinct associations.

EligibleRequest/EligibleReply additionally retain one immutable
`OwnershipObservation` made from the exact root checked by that invocation.
Content proves bytes relative to the association; it makes no root observation.
When these values are combined, the eligible value supplies the sole root
observation and must equal the final admitted observation. Values from another
signature/context invocation fail association even with identical UUIDs/bytes.
R03a3 must consume the eligible/content pair, compare the current observation,
and atomically advance replay plus reserve admission. No result is a positive
cache, and none authorizes a different root or later exchange.

## 1. R03a2b1, signature possession and signed request

Own new `server/src/node/auth/verify.rs`, private `verify/tests.rs`, and the
minimal module/reexports in `server/src/node/auth.rs`. Own only the enrollment
claim getters listed below in `auth/wire.rs`; no validators or credential
implementation change. Depends on reviewed R03a1 and R03a2a.

Implement synchronous concrete APIs returning `Result<_, auth::Error>`:

```text
verify_enrollment(bytes: &[u8]) -> EnrollmentProof
verify_request(auth_hex: &str, signature_hex: &str,
               actual: &ActualRoute) -> SignedRequest
```

EnrollmentProof privately owns `enrollment: Enrollment`, `public_key: [u8;32]`,
and `verifier: [u8;32]`. Decode first, reconstruct unsigned_bytes, verify
`b"meshdb.enroll.v1\0" || unsigned_bytes` with that public key and proof, then
compute worker_verifier over the checked cluster/node/incarnation/session/key.
Expose borrowed `claims() -> &Enrollment`, and fixed-byte `public_key()` and
`verifier()` accessors only. C03 obtains cluster_id/node_id/request_id/
incarnation/session_id from existing getters. Add read-only `endpoint() -> &str`,
`operation_slots()/partition_slots()/queue_size() -> u64` to Enrollment.
C03 can encode verifier bytes as lowercase hex for the existing Session field;
it cannot construct EnrollmentProof from claims or a verifier.

This proves signature possession only. The API enrollment route still owns
bearer authentication, initialization/storage evidence and fresh root loading.
A16 registration_plan owns exact launch/template/capacity/phase checks; its
owner-checked CAS and uncertainty recovery are separate. Neither this proof nor
an enrollment reply proves readiness or permission to execute.

For requests call Auth::decode with the actual route, decode_signature, and
verify exactly `b"meshdb.http-request.v1\0" || auth.canonical_bytes()` against
`auth.sender().public_key()`. Canonical equality was already checked against
original decoded header bytes. Keep the complete record and bytes. Expose
`SignedRequest::auth() -> &Auth` for read-only claims; no detached proof parts.
This establishes syntax/signature only, before body polling, root authority,
permit, freshness, capacity, replay or operation history.

A private fixed response-domain verification function in verify.rs may serve
mini 2. It returns no independently usable signed-reply proof. Prefix+message
allocation is capped before allocation at 4096 header bytes or 16 KiB enrollment
bytes, with the exact fixed NUL-terminated domains. No arbitrary domain input.

Causal tests in the private child use literal reviewed vectors and independent
synthetic signatures where new claims are needed. Isolate altered signature,
wrong domain and altered canonical bytes; show canonical reordered header
rejection with an otherwise valid signature. Check enrollment claim getters and
verifier binding with a valid baseline. Add negative trait guards for opaque
proofs and retain wire's existing no-Deserialize regression. Reuse credential
vectors rather than repeating its HKDF, entropy, key-generation or codec matrix.

## 2. R03a2b2, exact content and retained reply association

Depends on b1. Own new `server/src/node/auth/verify/content.rs`, private
`content/tests.rs`, and additive definitions/functions in verify.rs and private
reexports in auth.rs. Own only needed wire getters: `Auth::route() -> Route`,
small-body challenge/permit_id/drain_id/initialized/closed/barrier/idle/
heartbeat_sequence, and EnrollmentReply identity/registration/api_public_key
getters. Keys return checked `[u8;32]`, identities UUIDs, booleans bool and
sequence u64. Add no setters/validators/Serde construction paths. No protocol,
job, engine, decision or payload source edits.

```text
check_request_content(signed: &SignedRequest, bytes: Vec<u8>) -> CheckedContent
RequestContext::for_send(auth: Auth, initial: Vec<u8>, target_root: &Control)
    -> RequestContext
verify_reply(auth_hex: &str, signature_hex: &str, actual_status: u16,
             expected: &RequestContext) -> SignedReply
check_reply_content(signed: &SignedReply, bytes: Vec<u8>) -> CheckedReplyContent
```

These return Result with static auth errors; preserve parsed remote ProtocolError
as data, never as an auth diagnostic source. `for_send` consumes an already
checked Auth made through Auth::decode and the exact outgoing body/CONTROL.
It uses the same private content checker as the receive path, without pretending
the unsigned local record is a verified incoming signature. The existing signing
method separately checks that the sending key/role matches this Auth. Transport
must send this retained canonical Auth and original bytes, without rebuilding
claims after constructing context.

Expose only `RequestContext::auth() -> &Auth` and `initial_bytes() -> &[u8]`
for signing/sending, and `SignedReply::auth() -> &ReplyAuth` for read-only
claims. Context construction selects expectations; callers cannot replace them.

RequestContext fields are `request: Arc<RequestAssociation>`,
`initial: Box<[u8]>`, `expected: ExpectedContent`, `target: TargetBinding`, and
`statuses: ReplyStatuses`, all private. ExpectedContent is a closed private enum:
Operation owns the parsed Request; Job owns the parsed complete JobEnvelope;
Readiness owns Readiness; Open/Close own Dependency; Drain owns wire::Drain.
TargetBinding owns the exact Receiver from Auth and the matching validated
Node.session.verifier from target_root, plus its root observation. Reject a
missing, retired or different target binding. This construction is association,
not a fresh-authority grant. Do not accept arbitrary digest strings, a caller's
"eligible" boolean, or an asserted worker role as context evidence.

A fresh API replica may know only the durable verifier, not a worker's public
key. That is sufficient exact key commitment: verify_reply recomputes
worker_verifier with reply Sender key and the retained cluster/node/incarnation/
session and requires equality to target.verifier. It also requires Sender.role
Worker, process_id == incarnation, and exact target identity. No key-discovery
protocol or requirement to recover a public key from its hash is introduced.
Current-root eligibility is checked separately in mini 3.

The caller must allocate one never-reused positive sequence across ALL routes
for the same complete sender/key and target session. R03a3 owns the checked
sender allocator and exhaustion behavior. Context construction cannot prove
this temporal property. Never replace a pending context or reuse its sequence
for another operation/admission/job/attempt/target. This invariant is essential
because ReplyAuth carries request_sequence and scope, not those execution IDs.

verify_reply decodes canonical ReplyAuth and verifies the fixed response domain.
Compare cluster, sequence, scope, full original Sender receiver including API
process_id/public_key and nullable identities, exact target/key commitment, and
actual HTTP status == signed status. Request Receiver is a different type from
reply receiver. Status expectations are selected internally by route: 200 for
small success or framed job result, 200 for operation success, and existing
protocol error statuses for bounded JSON errors. No caller supplies a permissive
status set. Retain full original Auth and expected content in ReplyAssociation.
Unsigned/pre-auth errors cannot yield SignedReply. EnrollmentReply is unsigned,
uses neither ReplyAuth nor RequestContext, and must match every retained
cluster/node/incarnation/session identity through the later trusted TLS/local
channel before pinning its API key. Its getters do not certify that channel.

CheckedContent privately contains the request association, exact owned bytes and
parsed ExpectedContent. CheckedReplyContent contains the reply association,
exact bytes and a closed parsed reply enum. Accessors expose borrowed checked
claims for later readiness/drain/controller use. Eventual consuming extraction
belongs to R03a3/owner wiring and must retain association; no reusable bare
"hash OK" token. Specifically:

- Bound first, hash the EXACT original bytes with ring SHA256, compare to Auth
  control_sha256, and only then parse. Operation also requires request_digest to
  equal that hash and parse_request.request_id == operation_id. Retain request
  UUID/digest and bytes unchanged, including whitespace/numeric spelling.
- Job uses MAX_CONTROL_BYTES and parse_job. Compare cluster, operation, job,
  attempt and request_digest to Auth. Keep admission_id/dependency_id in Auth;
  they match the later retained permit/context, never imaginary JobEnvelope
  fields. Replies hash their first result CONTROL then parse_result against the
  entire retained JobEnvelope, including snapshot/ordinal/task expectations.
- Small success bodies use the existing strict 16 KiB/depth-16 wire decoders.
  Compare echoed challenge, permit_id or drain_id exactly. Preserve closed:false
  and idle:false as valid observations. initialized and barrier must be true
  as existing decoders require; heartbeat_sequence is positive. Hashes do not
  establish a current barrier, permit occupancy or authenticated idle proof.
- Operation replies need a NEW private bounded envelope reader here. Cap at
  MAX_BODY_BYTES, require an object, strict unique known envelope fields, one
  complete JSON value and EOF. For status 200 decode request_id and data as
  Box<RawValue>, using deserialize_object; require the retained request UUID.
  Keep data opaque and exact. This is envelope validation, not a new success
  schema, ORM validator or validation of all CRUD results. Preserve the existing
  JSON recursion bound. Do not deserialize ExecuteResult or call Decision::decode.
- Non-200 operation/small/job transport errors use a strict private JSON error
  envelope reader. Require the request_id field, allowing null when no public
  request UUID is available; operation errors must echo the retained UUID when
      known. A null operation error becomes the distinct checked reply variant
      `UnattributedRejection`, preserving its status/error under this transport
      context without attributing an outcome to the retained operation UUID.
      It can represent rejection before body parsing; no wire fact proves when
      parsing occurred. Never treat it as a terminal operation response.
  Decode nested code/outcome as strings into existing ErrorCode/Outcome, reject
  unknown/duplicate fields and trailing data, require code.status_code() to
  equal actual/signed status. Preserve NotApplied versus Unknown, including
  encoder fallback errors; do not apply Decision's terminal-error whitelist to
  transport responses. Cap operation errors at 8 MiB and other small errors at
  16 KiB. A 200 job JobResult::Error remains framed CONTROL via parse_result.

Hash agreement establishes neither terminal DB outcome nor full stream success.
C02 retains terminality, publication and history; engine/reducer retain success
data semantics. Later payload checks retain rows, prepared metadata/digests,
DATA/PART_META ordering, STREAM_END and real HTTP EOF/error/trailer obligations.

Causal tests group exact context associations and content parsing. Copy a valid
reply across contexts differing only in operation, admission, attempt, target,
route, status or sender identity, using different legitimate sequences, and
require failure at signature-context or content comparison. Show identical
operation/job UUIDs do not allow mixing independently constructed proof pairs.
Change original CONTROL bytes without changing its advertised hash, then change
hash/signature consistently but mismatch parsed claims. Use real parse_result
against a changed full expected job. Test challenge/permit/drain echoes, false
close/idle observations, operation UUID/raw data and status/error outcome
preservation, malformed/trailing envelopes and cap+1. Do not repeat job/CRUD
matrices. These tests prove envelope association, not runtime delivery/outcomes.

## 3. R03a2b3, pure provisional membership and route eligibility

Depends on b2. Own new `server/src/node/auth/verify/eligibility.rs`, private
`eligibility/tests.rs`, and additive proof definitions/reexports in verify.rs /
auth.rs. No cluster, admission, store, frame or payload implementation edits.

```text
check_request_authority(signed: &SignedRequest, root: &Control,
    local: &ReceiverIdentity, pinned_api: &[u8;32]) -> EligibleRequest
check_reply_authority(reply: &SignedReply, root: &Control,
    expected: &RequestContext) -> EligibleReply
```

Return Result. ReceiverIdentity has private cluster/node/incarnation/session/
public_key fields; `new(cluster, node, incarnation, session, &WorkerSigningKey)`
captures the local boot key. It is local input, not enrollment/initialization
proof. pinned_api is retained trusted-channel enrollment material, never a key
from incoming Auth. API replicas share the derived key; their process IDs differ.
No single-controller data coordinator or permission cache follows from API role.

Validate the caller-supplied root and build OwnershipObservation outside locks.
Compare cluster and exact current node/incarnation/session for each worker,
reject retired/lost/stopped, recompute worker_verifier against current Session.
Compare the receiver both to durable state and local ReceiverIdentity, including
its local key verifier. API Sender requires exact pinned API key; worker key
claiming API fails even if its signature is valid. The root is caller-supplied;
these synchronous helpers cannot certify GET freshness or runtime liveness.

EligibleRequest privately owns association, root observation, exact local
binding, and a closed `PendingLocalChecks` selected by route. EligibleReply owns
the reply association, current root observation, and corresponding retained
context requirements. It must compare RequestContext's request association,
target binding and expected identities to the reply, then check the current
worker key/phase again. Replacement or retirement since send rejects the reply,
even if signature and old context remain valid. For a worker original sender,
check its current binding too; API role remains the retained original API key.

| Route | Durable phase check | Pending final local/context check |
| --- | --- | --- |
| Operations | Receiver verified/ready; API sender pinned | Admission still open; operation capacity |
| Readiness | Receiver starting/registered or ready/verified; API pinned | Same live boot, initialized endpoint/storage evidence and exact challenge |
| Open | Receiver verified/ready; worker coordinator current | Barrier open, live operation binding, matching exact existing open retry or new permit room; closing cannot reopen |
| Jobs | Receiver verified ready or draining; sender verified ready or draining | Exact pre-barrier open permit binding; no closing/new permit or operation-slot acquisition |
| Close | Current worker bindings; ready or draining, including registered draining recovery | Exact retained binding if present; absent is allowed only as closed:true observation; closing refuses new jobs |
| Drain | Receiver exact durable draining, registered or verified; API pinned | Parsed drain_id equals root drain.id AND retained local drain/barrier identity; no new ready requirement |

A draining coordinator sends only dependencies of retained admitted operations,
including close. A07 trusts that coordinator's live-operation claim; this helper
cannot prove remote occupancy. A new open still requires a ready target with an
open barrier. A draining target refuses new opens, including for old operations;
an exact retry cannot reopen a closing permit or bypass its barrier. Header
eligibility remains provisional until those local checks. Jobs require
both endpoints verified; a merely registered draining recovery can drain but
cannot execute. A ready or draining job target must have the actual open permit,
including the implicit self permit. Bind coordinator node/incarnation/session,
operation_id, admission_id, request_digest and permit_id to Auth/content/context.
No timestamp or operation UUID establishes pre-barrier order.

The private pending enum records the exact IDs/digest/identity to compare; it
has no constructor from a caller's bool and no method that returns an accepted
admission. Its variants include NewOperation, Probe, Open, Job, Close and Drain.
Readiness/drain values expose checked claims with their association so C03/C06
can compare retained challenge/resource/heartbeat evidence; they do not create
ReadinessInput authority or confirmed_idle themselves. Reply eligibility for a
previously admitted operation/result allows verified draining bindings and
requires retained accepted-work/context evidence at final acceptance. Applying
new-operation ready-only rules to such replies would discard valid draining work.

Provide auth-private `EligibleRequest::matches_content(&CheckedContent) -> bool`
and the reply equivalent, using exact Arc identity, plus borrowed observation
and typed pending-check access for the future replay/admission owner. These
methods construct no accepted proof. Checked content provides borrowed typed
`operation()/job()/readiness()/dependency()/drain()` accessors, returning Option
for the closed route alternatives; reply accessors similarly expose parsed
result/small/error claims. No accessor detaches mutable records from a proof.

R03a3/R04 finalization compares this observation to the receiver's monotonic
complete-root observation, rejects superseded roots/equal-revision divergence,
checks unchanged local boot and the selected pending predicates against CURRENT
local permit/barrier/capacity, then consumes replay and reserves ownership under
one short lock. All fallible admission checks precede consume. The future owner
must supply the retained permit/operation evidence; these minis cannot manufacture
it. Root ordering, actual GET, queue-start/result rechecks and capacity are later
owners, not implicit facts of EligibleRequest. A10 guard/generation/owner/intents
remain additional obligations for controller effects and C03/C06 CAS.

Private tests use real generation-2 root encode/decode and synthetic keys. Start
with valid signatures and roots, then change only current session/key,
lost/stopped state or retired replacement and require rejection. Test worker key
claiming API, local receiver mismatch, readiness starting versus draining, and
valid verified draining job/close/accepted-result exceptions with pending exact
permit predicates. A wrong/missing/closing permit cannot become a final admission
from these provisional values. Check mixed invocation and differing root
observations at the proof-combination contract. These tests establish pure
eligibility/pending obligations; actual barrier/replay/capacity races belong to
R03a3/R04 tests, with deterministic gates rather than sleeps.

## Integration and evidence limits

Three tasks suffice. Mini 1 owns signature proof construction, mini 2 adds bounded
content/context checks, and mini 3 adds root/phase predicates. Each gets separate
specification then quality review and parent-assigned sequential edit/build time.
No implementer receives the whole auth/runtime module. Use private child unit
tests. Synthetic ring test keypairs can sign test messages without exposing the
credentials module's private seed constructor. Reuse literal vectors and
wire checked byte entry points. If a root fixture needs sharing, use one narrow
cfg(test) import/fixture visibility within cluster tests; do not export a public
facade. No storage Fake is needed for pure helpers.

Hashing, parsing, root validation/encoding/observation and context preparation
are finite bounded coordinator CPU/I/O commands, outside short locks and Tokio
polling. Source/allocation caps are not measured RSS guarantees. The future A15
prelude supervisor retains bounded transport admission through pending submission,
accepted work, cancellation and settlement. It reads one first CONTROL through
the SAME FrameReader, validates it before BeginRequest/BeginResult or scratch,
and later moves memory-only checked CONTROL exactly once into registered payload
ownership after final admission. Existing payload_stream::drive currently reads
and parses that CONTROL itself; its future coordinated handoff must accept the
checked record and advanced reader instead of rereading or reserializing it.
Keep body errors/EOF, remaining payload validation, cleanup and publication owners.
PayloadOwners admission alone is not authentication or dependency authorization.
None of those runtime changes belongs to these minis.

No essential protocol decision remains. The bounded raw operation-response
reader and durable verifier commitment above resolve actual missing seams without
changing accepted encoders/schema. Cross-route sequence uniqueness remains an
explicit R03a3 sender obligation; until wired, context tests cannot establish
transport replay safety. Fresh GET and root-observation ordering remain R03a3/R04
integration obligations. Do not check off R03a2b or distributed acceptance from
this design or pure-helper evidence.

This child read the applicable notes, rubric and actual wire/credentials,
protocol/response/decision, job, member/state/controller and frame/payload seams.
Only this new note was written. A scoped whitespace/final-newline check is the
only validation. No implementation, Cargo/build/test/probe, dependency fetch,
network/cloud/secret/.env access, delegation, branch/worktree or commit occurred.
No implementation or test guarantee is supplied.
