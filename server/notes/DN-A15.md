# DN-A15 private credentials and authentication minis

Refine adopted [A07](DN-A07.md), including its seven corrections, into three small implementation assignments after reviewed R01/R02, without implementing enrollment transitions, HTTP or lifecycle.

Design only, adopted by the parent after independent review and a passing re-check of the pre-admission validation ownership correction. [Shared contracts](../../plans/distributed-contracts.md) and the [R03/C03 rubric](../../plans/distributed-nodes.md) remain authoritative. Preserve A05 member fields, A10 authority guards and A11/A14 retained material. No essential parent protocol decision remains. The parent serializes module declarations and Cargo ownership. The approved direct `ring = "=0.17.14"` dependency and root lock entry are now implemented and reviewed in R03a2a; following verification work reuses that dependency. This note grants no concurrent Cargo ownership or additional dependency change, and design adoption alone establishes no implemented behavior.

## Existing boundaries

`node/job.rs` provides `parse_job`, `parse_result(bytes, expected)`, bounded encoders and semantic identity validation. `JobEnvelope` has no admission/dependency fields; preserve it. `FrameReader::next_frame` validates kind/declared length before allocation and accepts STREAM_END only with actual body EOF. `payload_stream::drive` currently reads the first CONTROL itself and immediately hands its bytes to `Command::BeginRequest` or `BeginResult`. Its `receive_*` functions already require a `PayloadLease`; they are not a header authentication entry point. `PayloadOwners::try_admit[_until]` registers supervised ownership, not durable membership or dependency authorization.

`cluster/member::{Node,Session}` validates record-local shape only. `Session` remains `{id,verifier}`. `Control::decode/validate` checks records and cross-record invariants, not current authority. Existing `startup_material::files::Auth` retains effective admin/data/enrollment strings without Serialize and with redacted Debug. Derive from that retained admin string later, never reopen a file or derive from its configuration fingerprint. A14 identity encoding uses lexical fields; A07 wire encoding uses its displayed field order. Share bounded-writing mechanics only, never substitute the identity serializer's ordering for Auth.

## 1. Fixed private records and canonical bytes

New `server/src/node/auth.rs`, `auth/wire.rs`, `auth/wire/tests.rs`. Parent adds only `pub(crate) mod auth;` in `node/mod.rs`; wire remains a private child. All DTO fields stay private. Expose crate-private validated records and concrete constructors/accessors only where later callers need them. No public library facade or integration-test visibility expansion.

Implement exactly these A07 records, with fixed Serialize declaration order, including nested fields:

```text
Sender = {role,process_id,node_id,incarnation,session_id,public_key}
Receiver = {node_id,incarnation,session_id}
Auth = {version,cluster_id,scope,sender,receiver,sequence,method,path,
        operation_id,admission_id,job_id,attempt_id,request_digest,
        control_sha256,dependency_id}
ReplyAuth = {version,cluster_id,request_sequence,scope,sender,receiver,
             status,control_sha256}
EnrollmentUnsigned = {version,cluster_id,node_id,request_id,incarnation,
                     session_id,public_key,endpoint,operation_slots,
                     partition_slots,queue_size}
Enrollment = EnrollmentUnsigned fields followed by proof
EnrollmentReply = {version,cluster_id,node_id,incarnation,session_id,
                   registration,api_public_key}
```

Reply sender is the complete worker Sender; reply receiver is the complete original Sender, not Receiver. Request receiver is Receiver. EnrollmentReply registration is exactly `registered|verified`. Version is exactly 1. Small bodies are the A07 readiness, permit and drain records in their displayed order, including required reply booleans and positive heartbeat sequence. EnrollmentReply is unsigned and must be associated with every retained enrollment identity through the trusted channel before pinning its key. It never goes through ReplyAuth.

Use object-only strict nested decoding, `deny_unknown_fields`, duplicate rejection, required-null adapters, canonical lowercase hyphenated UUIDs, fixed lowercase hex32/hex64 and string-only enums. Reuse `protocol::deserialize_object` and crate-private `member::{uuid,nullable_uuid,endpoint}` where applicable. Validate the no-prefix URL rule as startup_config does. Exact integer tokens must be decimal digits only, within u64, with positive sequence; reject signs, fractions, exponents, overflow and Serde marker objects. Do not parse through Value or unchecked tagged-content buffers. Reuse prior strict codec regression cases rather than recreating their full matrix.

The future HTTP adapter checks header multiplicity, method, literal path, query absence, role route, dependency header and framing. Its inputs to auth are borrowed bounded header values plus the actual method/path and optional dependency UUID, never an unchecked route selected from Auth alone. MeshDB-Auth hex is at most 8192 bytes, even-length, before decoding at most 4096 bytes. Signature is exactly 128 lowercase hex bytes before decoding 64. Bound nested depth to 16 before recursive DTO allocation. Re-encode incoming Auth/ReplyAuth and require byte-for-byte equality to the decoded bytes, including order, spelling, escapes and lack of whitespace. Sign those same bytes. Enrollment proof instead signs reconstruction of EnrollmentUnsigned after strict validation as A07 specifies.

Use bounded writers from the outset. Enrollment, replies and other small JSON exchanges cap at 16 KiB/depth 16; endpoint caps at 2048 ASCII bytes; capacities are 1..256 and queue 0..4096. Operations retain the public 8 MiB cap. Jobs retain separate 16 MiB CONTROL/PART_META and 64 KiB DATA caps. No allocation follows an unchecked declared length; no row/Parquet bytes enter Auth. No auth error includes a header, body, key, verifier, digest, token, path value or dependency error source. Use fixed internal categories `Invalid`, `Limit`, `Replay`, `Exhausted`; adapters later map A07 status/outcome semantics.

Pin the route claims in one closed enum, not configurable policy:

| POST path suffix under `/internal/v1/` | Scope / sender | Nonnull execution claims |
| --- | --- | --- |
| `operations` | operation / api | operation_id, request_digest |
| `jobs` | job / worker | operation_id, admission_id, job_id, attempt_id, request_digest, dependency_id |
| `readiness` | readiness / api | none |
| `dependencies/open` | dependency_open / worker | operation_id, admission_id, request_digest |
| `dependencies/close` | dependency_close / worker | operation_id, admission_id, request_digest |
| `drain` | drain / api | none |

All other claims are required null. `control_sha256` is always nonnull. Worker process_id equals incarnation and node/incarnation/session are nonnull. API identity fields except process_id/public_key are null. Jobs require exactly the matching MeshDB-Dependency value; it is absent on all other routes. Enrollment is the separate API-listener bearer route and never accepts these credentials as enrollment authorization.

Private tests pin literal canonical Auth/reply/enrollment bytes, required nulls and nested order. Add representative strict failures, especially a valid signature over reordered incoming Auth, exact-u64 boundaries, marker objects and odd/uppercase hex. Exercise actual bounded writers at cap and cap+1 and modest oversized declarations before allocation. These are auth-contract tests, not another CRUD or all-fields matrix.

## 2. Memory-owned keys and pure verification

Depends on mini 1 and the parent's Cargo prerequisite. New `auth/credentials.rs`, `auth/verify.rs` and their private `tests.rs` children; narrow re-exports through auth.rs only. Keep WorkerSigningKey and ApiSigningKey distinct, owning ring key pairs, with no Serialize/Deserialize/Clone or revealing Debug. Constructors expose only fixed public-key bytes and domain-specific signing methods. Do not provide a general sign-arbitrary-domain API. Seeds remain memory-only; no secret persistence, PKCS8 export or zeroization guarantee is proposed.

Worker construction uses `SystemRandom::new()` and `SecureRandom::fill(&mut [u8;32])`, then `Ed25519KeyPair::from_seed_unchecked`. Retain boot incarnation/session/key across enrollment retries. Compute verifier with ring SHA256 over exactly `b"meshdb.worker-verifier.v1\0"`, cluster/node/incarnation/session `Uuid::as_bytes()` in that order, then public key bytes. Never hash UUID text or use the verifier as a bearer/MAC key.

API derivation uses `hkdf::Salt::new(hkdf::HKDF_SHA256, cluster.as_bytes()).extract(admin_token.as_bytes())`, `expand(&[b"meshdb.api-signing.v1"], Len32)` where the private KeyType returns 32, then `fill` into a 32-byte seed and construct the Ed25519 pair. The info has no NUL. The retained admin secret has A11's deployment entropy obligation; syntax cannot prove entropy. Do not use enrollment/data secrets or add configuration. API replicas with the same cluster/admin derive the same key, while their process_id is fresh per boot.

Use ring `KeyPair::public_key`, `sign` and `UnparsedPublicKey::new(&ED25519, public_key).verify`. The exact signature inputs are domain plus canonical bytes, with one terminating NUL in each of `meshdb.enroll.v1`, `meshdb.http-request.v1`, `meshdb.http-response.v1`. Signature buffers are bounded header/enrollment bytes only. Use ring SHA256 for exact CONTROL/body hashing. No new crypto primitive, dependency, TLS identity or PKI is designed.

Pin these small synchronous boundaries with private fields and single-use, non-Clone results:

```text
verify_enrollment(bytes) -> EnrollmentProof
verify_request(auth_hex, signature_hex, actual_route) -> SignedRequest
verify_reply(auth_hex, signature_hex, expected: &RequestContext) -> SignedReply
check_request_content(signed: &SignedRequest, bytes) -> CheckedContent
check_reply_content(signed: &SignedReply, bytes) -> CheckedReplyContent
check_request_authority(signed: &SignedRequest, root: &Control,
                        local: &ReceiverIdentity, pinned_api: &ApiPublicKey)
    -> EligibleRequest
check_reply_authority(reply: &SignedReply, root: &Control,
                      expected: &RequestContext) -> EligibleReply
```

`EnrollmentProof` establishes only key possession and strict identity claims. A later C03 transition additionally needs fresh LoadedControl/token, enrollment bearer authentication, controller OwnershipGuard, retained launch/template/capacity evidence and CAS uncertainty handling. It implements A07 exact retries, late/draining registration and retirement checks separately. A proof never certifies membership, enrollment commit or readiness.

`SignedRequest` and `SignedReply` establish only syntactic/cryptographic validity. Pure authority functions compare caller-supplied validated Control and in-memory identities/keys. They cannot certify that a root was freshly fetched. Later adapters must freshly load Control per exchange after signature verification, fail closed on storage uncertainty, and reject superseded observations at the admission boundary. Eligible values belong only to that invocation, never a positive membership cache. No store access, callback, task startup, lease acquisition or OwnershipGuard mutation occurs in auth.

Authority checks compare cluster, durable/local receiver node/incarnation/session and nonretired current worker binding, recompute verifier from the claimed key and durable session, and apply route eligibility. API role requires equality to the enrollment-pinned API key. Operations/opens require verified ready target; readiness allows registered starting or ready, never draining; jobs permit verified ready or draining with a retained permit; close permits draining; drain requires exact durable draining target/drain_id. Worker senders must be eligible current members, with draining senders restricted to retained operation dependencies. The content/permit check completes drain_id and dependency eligibility, which header-only proof cannot establish.

`RequestContext` retains the complete original canonical Auth, actual route/status expectations, receiver and original sender, plus expected JobEnvelope or small-body challenge/permit/drain identity. Reply validation checks cluster, sequence, scope, HTTP status, exact worker sender/key/target and exact original sender object. A unique never-reused sequence for each sender/target across routes associates the reply with all original request claims. It does not need new response fields. Check original operation/admission/job/attempt/digest/dependency against retained context and exact content; never associate by scope or job_id alone. A copied reply from another route, target, role, admission or attempt fails even if its signature is valid.

Hash exact operation bytes and require request_id/operation_id and request_digest agreement after `parse_request`. Hash exact first job CONTROL payload before `parse_job`/`parse_result(expected)`; compare all Auth job identity fields and retained permit binding. Snapshot/ordinal/table/task remain bound by CONTROL hash and existing validators. For replies hash their exact small/operation body or first result CONTROL; validate echoed challenge/permit/drain and result identities. CONTENT hash success does not establish row/file semantics, full stream completion or database outcome.

Private tests use an independent published Ed25519 test vector, fixed HKDF/verifier/signature expectations and literal canonical message bytes. A private `#[cfg(test)]` seed constructor supplies deterministic synthetic keys. Do not generate expected signatures/digests with the production function under test. Include changed key/cluster/UUID-byte scope, target, role, signed bytes and domain. Synthetic roots must show a cryptographically valid old key failing after replacement/lost/retirement, and an API role signed by a worker key failing. Replies test changed status, sequence and retained request association; wrong CONTROL bytes must fail before any semantic-owner handoff. State fixed-vector provenance honestly; proposed cases have not run in this design task.

## 3. Replay state and final admission contract

Depends on minis 1/2. New `auth/replay.rs`, `auth/replay/tests.rs`; auth.rs exports only concrete replay operations and opaque content/request context needed by the future admission owner. Implement no listener, dependency permit registry or storage API here.

`ReplayState::new(local_receiver)` fixes the receiver boot identity. Its non-evicted maps key worker entries by complete sender identity/public key and API entries by process_id/public key, always scoped to this receiver session and cluster. Limits are 2048 worker sessions and 128 API processes including retired/ineligible entries already seen. New identity exhaustion rejects without eviction or mutation; existing identities may still advance. Store only identity and highest accepted positive u64, no bodies/secrets. An optional `would_reject` lookup is advisory. Only `consume(&mut self, eligible: EligibleRequest, content: CheckedContent) -> AcceptedAuth` atomically compares and advances. Both opaque inputs retain association to the same complete canonical Auth; mismatched inputs fail without mutation. Invoke it through the receiver admission mutex, never a replay-only lock followed by a separately unlocked admission. `consume` checks the fixed receiver and cannot lower/reset/remove a high-water. AcceptedAuth retains request/reply context, grants no durable authority and is not Clone or serializable.

The exact future integration order is:

1. Acquire bounded transport/header admission. Validate header syntax/canonical bytes/signature and actual route. No body poll, spool, compute admission or history lookup yet.
2. Freshly GET/validate Control outside all admission locks. Check durable/local identity and effective eligibility using that root and in-memory pinned key. Storage uncertainty rejects. No reusable authorization cache is installed.
3. Poll only bounded initial content, hash its original bytes, parse and cross-check claims. Jobs read one FrameReader CONTROL with its existing declared bound; they do not collect DATA. Small routes check their bounded bodies. Retain exact signed request/context through the final step.
4. Under one short receiver admission lock, reject any superseded root observation or changed local identity/barrier/permit. Re-evaluate eligibility against the admitted root and current local state; check all content claims and available route capacity first. Then call ReplayState::consume and synchronously reserve the accepted request/permit owner before releasing the lock. No await, storage I/O or fallible capacity acquisition follows consumption inside this transaction. Concurrent equal-sequence requests have one winner. Concurrent sequence 12 before 11 rejects 11. Failed signature, authority, body/hash, permit or capacity claims leave high-water unchanged.

Root freshness remains a caller obligation. The later admission owner records monotonic validated Control observations and rejects an older observation after a newer one; equal revision with unequal complete contents is an integrity failure. It must not let a slow old-root request reinstall a replaced session. A revocation racing after the fresh linearizable GET does not retroactively cancel an otherwise admitted request under A07. Recheck eligibility before queued execution and result acceptance. A10's controller guard/generation and fresh owner/intent authorization are additional requirements only for controller effects; neither API signing nor replay state grants them.

Pre-body replay checks cannot reserve a sequence, and two requests may concurrently read bounded initial content. Transport admission bounds that cost. Final consume occurs only after authenticated content and eligibility checks. Once consumed it is never rolled back on cancellation, supervisor setup failure or uncertain effects. A rejection describes this attempt only; it proves nothing about earlier same-operation attempts. Sender sequence allocation is checked increasing across all routes for each target, stops at u64 exhaustion and never wraps. Mutations preserve request UUID/exact bytes and use C02 reconciliation after uncertainty; pure jobs retain existing logical/attempt identity rules. New-sequence retries require the route's existing permission and are never automatic transport retries.

The later HTTP/owner slice must insert the CONTROL hash/claim gate before `payload_stream::drive` submits BeginRequest/BeginResult or allocates semantic scratch. The future prelude retains bounded transport admission under supervised cancellation ownership before reading initial content or submitting validation. Run body/CONTROL hashing and semantic parsing as finite commands on the separate coordinator CPU/I/O pool. Cancellation settles pending submission and accepted work before releasing admission. Return only memory-owned checked content. Final admission performs bounded identity/permit/capacity checks without parsing or hashing, then transfers the checked CONTROL exactly once with the same FrameReader and preserved body errors/EOF into registered payload ownership. No raw disk owner crosses this handoff. These remain future coordinated integration obligations; the pure minis implement no supervisor or HTTP adapter. TLS, trailer rejection, DATA/PART_META validation, final EOF and cleanup owners retain their existing A07/R01 obligations. Full result acceptance is later than initial hash validation.

Private replay tests use two callers gated at the real shared-mutex consume boundary and assert exactly one success for the same sequence. Exercise reordered sequences, sequence max/exhaustion, receiver restart mismatch, unchanged state after invalid claims, and full-map refusal without evicting an old identity whose replay must still fail. Use small injected map limits plus assertions of production constants. Reuse codec/frame malformed-length, trailing/missing-EOF and owner cancellation evidence; do not repeat those suites or prescribe external integration tests for crate-private auth. Later HTTP/C03 assignments supply actual header-before-body, fresh-store and accepted-owner composition evidence.

Only this new note was written. Read-only inspection covered the adopted contracts/rubric, A05/A07/A10/A11/A14, reviewed typed job/frame/owner/stream handoffs, actual member/state/material interfaces, manifests/lock and cached ring 0.17.14 source. No implementation, Cargo/build/test/probe, network/cloud, environment/profile/secret access, delegation, branch or commit occurred. No implementation, test or runtime guarantee is claimed.
