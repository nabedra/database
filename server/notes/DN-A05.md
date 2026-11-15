# DN-A05 durable control records and transitions

Design only, 2026-11-10. [Shared contracts](../../plans/distributed-contracts.md) supersede historical A02/A03 proposals and A04's completed-result cache. This note pins C03..C07/G02 persistence and private authorization. It adds no public API. Keep five-argument `serve`, implemented Python `Cluster` records/errors, the separate API routing process, and identical workers. One conditional backend holds all keys, using HTTP or optional feature-gated S3.

## Record rules and limits

Parent reconciliation: accepted for implementation. Use a two-second tolerance
when detecting local wall-clock discontinuity against monotonic elapsed time;
on larger discontinuities deny renewal-dependent dispatch/authorization until
authority is re-established by a fresh owner-checked CAS. This is a local
failure detector, not a claim of globally bounded clock skew or lease-expiry
fencing. Accepted [A07](DN-A07.md) now pins session signatures and HTTP framing;
it supersedes the original secret-hash verifier and enrollment-derived shared
message key. Node.session fields stay unchanged. No credential rotation or
lifecycle archive is added implicitly.

Accepted [A11](DN-A11.md) adds the required worker semantic fingerprint and
sets control Config generation to 2 without changing the root version. It pins
loaded material identity and compatible local-path relocation. Generation-1
control configurations fail incompatible without writes or migration. The
pending C08c implementation owns the exact codec and synthetic fixture update;
prior codec delivery notes record the earlier generation they actually tested.

All records below are strict JSON objects, version 1, with exactly the listed fields. Every nullable field is required, including when null. Reject duplicates at every nesting level, unknown fields, arrays in place of records, nonfinite values, fractional/exponent integer spellings, and unsupported versions. UUID means canonical lowercase hyphenated UUID; generated identities are fresh random UUIDs. Digest means 64 lowercase SHA256 hex characters. Integers are checked u64 unless a narrower range is given. Counters never wrap or saturate into an apparently successful transition. Exhaustion returns a sanitized limit failure without mutation or callback.

Names retain `[A-Za-z_][A-Za-z0-9_]{0,63}`. Resource IDs retain A03's nonempty, control-free UTF-8, at most 256 bytes. URLs are validated absolute HTTP(S), no userinfo/query/fragment, at most 2048 ASCII bytes; advertised ports cannot be zero. Machine error codes retain `[A-Za-z0-9_.:-]{1,256}`. No raw provider diagnostic, bearer secret or secret-file content enters records, reprs, status or SDK logs.

Use accepted [A08's first-stop reservation contract](DN-A08.md): a 32 MiB encoded control root, 64 KiB template, 4 KiB Timing and 8 KiB heartbeat. Job CONTROL/PART_META stays at 16 MiB. Retain at most 2048 nodes, 3072 intents and 4096 combined records. For retained counts N/I, every loaded root and candidate checks bounded actual encoding and `B = 256 KiB + 8 KiB*N + 4 KiB*I <= 32 MiB`. The per-record 8/4 KiB maxima reserve all permitted mutable-field growth; the fixed allowance covers maximum global fields, template and separators. Derive C first-stop credits from exact retained managed launch/stop bindings as A08 specifies. New admissions additionally require `I+C <= 3072`, `N+I+C <= 4096`, and `B + 4 KiB*C <= 32 MiB` on the complete candidate. A first stop atomically converts its node's credit into an intent, preserving those augmented budgets; it needs no extra free admission slot. Updates of retained records use their reserved growth. Stop failure/completion never restores a credit, and ambiguous launches retain theirs. Terminal no-effect/termination witnesses must follow A08's irreversible evidence rules; a node phase, timeout or inventory absence alone is insufficient. Admission checks are not decoder phase requirements. These rules permit the initial 1024-node fleet and all its first stop records without promising unlimited churn or provider success.

Public status remains the exact version-1 A03/Python dictionary, with its policy revision in `revision`, sorted identities, no secrets, and no more than 4096 combined records. Encode the entire response envelope under 1 MiB. A 1024-node policy is legal; arbitrary maximum-length resources and retained records need not fit status. Oversized status returns existing `limit_exceeded`/413 without truncation. New admissions should check projected status too; mandatory recovery and reserved first-stop conversion must still persist safely and may make status unavailable at that limit. Never discard recovery state to make status succeed.

## Exact root and member schemas

`cluster/control.json` is Create once, then Match only. Its exact shape is:

```text
Control = {
 version:1, cluster_id:UUID, name:identifier,
 config:{generation:2, storage_fingerprint:Digest, auth_fingerprint:Digest,
         template_fingerprint:Digest, lifecycle_fingerprint:Digest,
         worker_semantic_fingerprint:Digest,
         adapter_contract:"meshdb.callbacks.v1"|null},
 provisioning:"managed"|"external", template:Template, timing:Timing,
 revision:positive_u64, change_id:UUID,
 policy:{revision:positive_u64, mode:"fixed"|"autoscale",
         desired:0..1024, min_nodes:0..1024, max_nodes:0..1024},
 controller:{owner_id:UUID|null, epoch:u64,
             lease_state:"released"|"held", lease_until_ms:u64|null},
 scheduler:{cooldown_until_ms:u64, backoff_until_ms:u64,
            failures:u64, low_since_ms:u64|null},
 admin_seq:u64, admin_head:UUID|null,
 nodes:[Node], intents:[Intent]
}
Timing = {
 heartbeat_ms:positive_u64, expiry_ms:positive_u64,
 readiness_ms:positive_u64, drain_ms:positive_u64,
 lease_ms:positive_u64, renew_ms:positive_u64,
 stabilization_ms:positive_u64, cooldown_ms:positive_u64,
 retry_initial_ms:positive_u64, retry_max_ms:positive_u64,
 max_attempts:positive_u64
}
Node = {
 node_id:UUID, launch_id:UUID|null, resource_id:Resource|null,
 ownership:"managed"|"external", state:"starting"|"ready"|"draining"|"lost"|"stopped",
 incarnation:UUID|null, retired_incarnations:[UUID], endpoint:URL|null,
 operation_slots:1..256, partition_slots:1..256, queue_size:0..4096,
 registration:"unregistered"|"registered"|"verified",
 session:{id:UUID, verifier:Digest}|null,
 drain:{id:UUID, requested_at_ms:u64, ack_seq:u64|null,
        confirmed_idle:bool}|null
}
Intent = {
 request_id:UUID, node_id:UUID, launch_id:UUID,
 incarnation_id:UUID|null, resource_id:Resource|null,
 action:"start"|"stop",
 state:"pending"|"in_progress"|"reconciling"|"completed"|"failed",
 attempts:u64, created_at_ms:u64, deadline_ms:u64,
 retry_at_ms:u64, error_code:Code|null,
 template_fingerprint:Digest|null,
 delivery:{invocation_id:UUID, owner_id:UUID, epoch:positive_u64,
           command_sha256:Digest}|null,
 outcome:"resource_known"|"terminated"|"not_invoked"|null
}
```

`Template` has exactly bootstrap's `api_urls`, `listen`, `storage`, `tls`, `operation_slots`, `partition_slots`, `queue_size`, `partition_size`, excluding version, cluster/node/request IDs, advertise_url and enrollment_token. `api_urls` contains 1..16 URLs, `listen` is a validated host:port of at most 256 bytes, and partition_size is a validated unit-bearing string of at most 64 bytes. TLS is null or exactly `{cert_file,key_file,ca_file}`, each a nonempty path of at most 2048 UTF-8 bytes. `storage` is exactly `{driver,options}`. HTTP options are exactly `{base_url:URL,namespace:string,token_file:path,ca_file:path|null}`; S3 options are exactly `{bucket:string,namespace:string}` and require the s3 feature. Namespace uses the existing storage path guard, bucket is validated by the S3 adapter, and both are bounded to 1024 bytes. Paths are nonempty and at most 2048 UTF-8 bytes. Credential references must be file paths or the standard S3 credential chain, never inline credentials. All nested template JSON shares a maximum depth of 32 and its encoded 64 KiB bound. This is the single backend, not historical `control`/`objects` bindings.

Fingerprint input uses one Rust serializer: recursively sorted object keys, compact UTF-8 JSON, no optional omissions, integer decimal spelling. The exact outgoing-template fingerprint is SHA256 of `b"meshdb.control-template.v1\0"` followed by those complete bounded Template bytes. Other fingerprints use their own versioned domain labels. Storage fingerprint binds driver and canonical durable backend address/namespace, excluding replica-local credential paths. Auth fingerprint binds cluster ID and distinct admin/data/enrollment secret digests with role labels; require shared high-entropy secrets. TLS trust/certificate identities and all worker semantic settings enter template/config compatibility. File contents are never stored; secret digests are private metadata, never public diagnostics. A local path difference is compatible only when loaded identity/trust/credentials and durable backend identity match.

The creator persists one outgoing template, including its credential-path strings as adapter rewrite hints. Every compatible replica uses that persisted template verbatim for callbacks, never its own local paths. The adapter replaces those hints with target-local credential/TLS paths before worker startup. Rust must never open outgoing-template paths as API-local files. Each replica loads the same enrollment secret from its own auth_file and inserts it only into the in-memory bootstrap. Thus replicas reconstruct identical stable bootstrap values even when their local credential paths differ. Verify template_fingerprint against the stored template; compare the attaching configuration semantically after replacing credential paths with their loaded credential/trust identities. A different path alone cannot change that fingerprint or outgoing payload. Enrollment/template rotation is unsupported while attached in this version; an incompatible replica fails startup rather than silently changing pending commands. Custom callbacks must honor this same durable identity/idempotency contract; function objects are not fingerprinted or serialized.

Initial Create sets root/policy revisions to 1, fresh change_id, admin_seq=0/head=null, epoch=0/released with owner/deadline null, empty arrays, and scheduler times/failures zero with low_since null. Fixed policy has equal desired/bounds, including zero. Autoscale has `1 <= min <= desired <= max <= 1024` and starts at min. External mode has null adapter_contract and rejects autoscale. Attach verifies cluster/name/config/provisioning compatibility and preserves all policy, history, timers and intents regardless of the new serve `nodes` argument. A missing managed bridge prevents controller ownership; it does not reset the cluster.

Arrays are sorted by node_id/request_id, unique by those IDs. Non-null member resources/incarnations are unique. Managed nodes have launch_id; external nodes have null launch_id and resource `external:<node_id>`. A registered node has non-null incarnation/endpoint/session; unregistered has all three null. Verified registration requires resource identity and committed authenticated reachability verification; only verified nodes can become ready. Draining/stopped/lost never becomes ready from a heartbeat. Managed replacement uses fresh node/launch identities, and every process boot uses a fresh incarnation. Retain old managed stopped/lost members so their identities cannot be enrolled again. External nodes may preserve their operator's stable node_id across reboot: after the old incarnation is durably lost or safely stopped, enroll the new incarnation through an explicit CAS, retire/revoke the old session, reset drain/verification and require readiness again. `retired_incarnations` is a sorted unique array of at most 16 UUIDs, initially empty, disjoint from every current incarnation. Reject retired incarnation registration; at this bound refuse another replacement until a future durable archive exists. Same-incarnation registration retries may recover the existing binding but cannot replace its session or endpoint.

## Ownership and conditional writes

Accepted [A10](DN-A10.md) now pins the small ownership-candidate, single-Match
submission and local-certainty interfaces. Guard admission and preparation
share one explicit fresh proposal UUID and bind the expected complete root.
Only an eligible acknowledged acquisition/renewal installs local certainty;
exact read-back alone does not. Newer claim/admin/member roots preserve existing
certainty only when the complete controller tuple is unchanged, without
extending its original deadline. Pending ownership stamps are invalidated by
newer observations; late results, errors and cancellation act only on their
original stamp. A04 authorization also rechecks that its fresh read is still the
guard's high-water complete root. A10's detailed integrity, clock and overflow
rules apply; these design rules establish no implemented authority guarantee.

Keep `LoadedControl {root, token}` private and validated, like `LoadedClusterCatalog`. A transition always constructs a fresh root with revision+1 and a fresh change_id, preserves unrelated fields, and Match-writes its exact loaded token. Every controller mutation checks loaded owner_id/epoch against its caller, even renewals, callback claims/completions, membership writes and intent retries. G02 policy writes can originate on any authenticated API replica; they preserve loaded controller fields and still Match the exact token. Takeover and enrollment never use unconditional overwrite.

Acquire from released state or an eligible expired lease via CAS, incrementing epoch and setting a fresh process owner_id, held state and checked deadline. Renew preserves owner/epoch and advances deadline; relinquish validates ownership and clears owner/deadline to released while retaining epoch. Epoch zero is only the never-acquired state. CAS takeover fences subsequent control CAS from the old owner. Lease expiry alone revokes neither a pending control write nor a database catalog CAS. Root control ownership grants no exclusive database publication role.

Controller deadlines are Unix milliseconds for takeover eligibility, not worker-clock evidence. Use conservative skew handling and a local monotonic renewal deadline, stop dispatch/authorization on clock discontinuity or lost renewal certainty. Another replica can attempt takeover according to the persisted deadline; only its successful CAS establishes authority. Validate `heartbeat < expiry`, `renew < lease/2`, `retry_initial <= retry_max`, and checked timing additions; defaults retain heartbeat 5s, expiry 15s and readiness 300s. Recommend lease 15s/renew 5s, drain 300s, stabilization 60s/cooldown 30s, retry 1s..30s with 8 attempts. Takeover resets the *local* observation/stabilization evidence, never persisted backoff or attempt counts.

On Conflict reload and stop the proposed action immediately if owner/epoch changed; otherwise recompute from the new root within a bounded budget. An uncertain Create/Match is never blindly resent. Reload and recognize exact change_id for a still-current result; admin ambiguity uses the reachable ledger below. Ownership ambiguity permits no callbacks until a fresh successful owner-checked renewal CAS proves authority. For other transitions, inspect the durable node/intent phase and either adopt that state or make a new owner-checked CAS from the fresh token. A successful subsequent CAS fences an earlier still-pending write. A later root alone is not proof the uncertain transition happened. Overflow in acquisition, renewal, relinquishment or completion fails closed and stops local authority; no owner/epoch reset is allowed.

No controller lock spans storage waits or callback HTTP. The independent authorization listener must continue reading roots while the controller awaits Python, and renewals must keep progressing independently. A small local lock may protect a loaded snapshot but supplies no distributed authority.

## Lifecycle commands and private authorization

A start intent's request_id equals launch_id, has null incarnation, and pins template_fingerprint. A stop uses a distinct request_id, names the original launch_id, exact resource and its incarnation when known, and has null template_fingerprint. External resources never receive provider stop commands. A managed stop for a never-registered resource may have null incarnation only with a persisted exact launch/resource binding.

Use A04's exact command fields and payloads. Persist a delivery claim before dispatch, increment attempts, set in_progress and store a fresh invocation_id, current owner/epoch and SHA256 of the exact serialized command bytes. For start, intent_id=launch_id and target is `{node_id,request_id:launch_id,incarnation_id:null}`; payload is the A03 start request with the reconstructed bootstrap and advertise_url null. For stop, intent_id is the stop UUID, target.request_id is launch_id, and payload is exactly `{resource_id}`. Retries/takeover keep the same intent/launch/target/template; only delivery identity, owner and epoch change. The payload must be validated/reconstructed before its digest is claimed, not accepted merely because its hash matches an untrusted proposal.

Immediately before callback entry, Python sends those exact command bytes to `/private/v1/authorize`. Rust authenticates A04's lifetime authorization token, freshly GETs Control, checks cluster/current local API owner/epoch and local unexpired, certainly renewed lease, then checks the persisted in_progress delivery, action, target, immutable payload/template and command digest. It also revalidates phase eligibility: start must still be an unresolved launch; stop must have the exact safe-drain proof described below. Hash comparison covers exact bytes including invocation/owner/epoch and the in-memory enrollment token. Store only digests, never those command bytes. Authorization returns A04's invocation/digest success; Python compares both and invokes immediately in its already-held slot. No authorization tickets or completed Python cache. Redelivery after completion requires new authorization and provider idempotency.

Inventory remains A04's transient null-intent/null-target/empty-payload command, authorized by a fresh owner/epoch/lease check and exact-byte digest. It has no persisted lifecycle identity and grants no start/stop authority. It cannot substitute for the persisted delivery claim of an action. Use the reserved inventory thread and strict complete-response validation; invalid/duplicate inventory gives no usable observation. Structurally valid unknown resources are quarantined, never registered, dispatched to, or stopped automatically. Duplicates degrade provisioning without attaching either conflicting resource. Persist only sanitized degradation/backoff, not the raw conflicting inventory.

`pending` has no dispatched effect. Claim commits `in_progress`. After transport loss, timeout, uncertain entry or takeover of an unfinished claim, move to `reconciling`, preserve identity, and run inventory before any redelivery or replacement. Positive exact inventory recovers a lost start result. A known resource can complete start with outcome resource_known before readiness; its nonready member still consumes capacity. Stop completes with outcome terminated only after a positive terminated record or a separately documented authoritative exact-resource absence guarantee. Stop callback return alone is not termination proof. Ordinary inventory absence never proves an ambiguous start failed.

`failed` requires definite terminal failure with no unresolved effect: outcome not_invoked with durable pre-entry evidence and no prior ambiguous delivery, or definitive termination evidence. Ambiguous exhausted/stalled work remains reconciling with its error_code, last delivery, retry time and attempts intact. It reserves capacity and remains subject to inventory reconciliation. This keeps it incomplete under the implemented Python wait predicate. Cap automatic callback redeliveries at max_attempts; keep reconciling inventory afterward. Persist exponential capped backoff, including scheduler-wide provider failures. Restart/takeover and repeated admin policies cannot reset it. Late positive evidence moves reconciling to completed; if capacity is no longer wanted, create a separate safe drain/stop intent for that same recovered resource. Never issue a replacement identity solely because launch/readiness deadline elapsed.

This slice retains completed intents and stopped/lost members in Control. No TTL, eviction or compaction is implemented. A later compaction may remove a record only after an immutable reachable archive preserves its full outcome, original launch/resource/incarnation/template identities and late-effect lookup, without forgotten IDs. Until that exists, hitting record/byte limits safely blocks unreserved allocations. A08's already reserved first-stop conversion and permitted growth of retained records use their existing room. Do not reuse admin-decision keys for unlinked lifecycle blobs or treat provider inventory alone as that archive.

## Registration, heartbeat and drain evidence

`cluster/heartbeats/<incarnation>.json` is Create then Match by that incarnation, using its loaded token. The exact record is:

```text
{version:1, cluster_id:UUID, node_id:UUID, incarnation:UUID, sequence:positive_u64,
 operation_slots:1..256, partition_slots:1..256, queue_size:0..4096,
 running_operations:u64, running_partitions:u64,
 queued_operations:u64, queued_partitions:u64,
 drain_id:UUID|null, accepting:bool}
```

Occupancies cannot exceed their corresponding capacities/queue_size. Capacity matches registration. Sequence increments for each observation, never wraps; writer uncertainty reloads the exact incarnation key and reconciles its sequence before another Match. On exhaustion the worker stops new admissions and needs a fresh boot/incarnation. No worker timestamp determines membership. Controller reads only keys named by membership; LIST is not authoritative. It tracks monotonic time since *sequence advance*, rejects regression, and grants a full expiry interval after takeover before declaring loss. A repeated sequence does not refresh freshness. Routers also require durable ready state and fresh observations; missing/corrupt metrics make metrics_fresh false and forbid automatic shrink.

Enrollment validates a managed launch or an externally allowed identity, enrollment credential, storage/catalog access, initialized resources and endpoint syntax. The current controller persists registration first. It then proves authenticated execution reachability to that exact incarnation and commits verified/ready in a separate CAS. Heartbeat or open TCP alone cannot do this. Registration before resource recovery stays registered/nonready. Durable draining/lost/stopped state rejects subsequent ready claims and old sequence advances. A late managed registration may bind only its still-eligible launch; stopped identities are never revived.

Minimum session metadata is Node.session's fresh id plus `SHA256("meshdb.worker-verifier.v1\0" || cluster_uuid_bytes || node_uuid_bytes || incarnation_uuid_bytes || session_uuid_bytes || public_key_bytes)`, as pinned by accepted A07. Each worker boot generates a random 32-byte Ed25519 seed before registration, keeps it in memory, and proves possession of its public key. Exact retries recover the existing binding after lost acknowledgements without replacing it or retrieving a secret from storage. The verifier is never a bearer or signing key. API replicas derive a separate Ed25519 signing seed through A07's HKDF-SHA256 construction from the existing admin secret, which workers do not receive. This replaces the earlier enrollment-derived shared message key: it could not distinguish API and worker authority. All receivers validate scoped signatures and fresh durable session/eligibility; the raw enrollment token is used only for enrollment. A07 specifies exact request/response binding, bounded replay evidence, TLS/local-HTTP trust limits, and late/draining registration rules. Those mechanisms are implementation work, not guarantees established by this schema note.

Drain first CASes state draining, fresh drain.id and requested time, then sends authenticated drain to the exact session/incarnation. That worker disables new operations/jobs while finishing accepted coordinations and their dependent partition jobs. A valid drain acknowledgement binds drain.id/session/incarnation and a heartbeat sequence at or beyond the barrier. Commit ack_seq and confirmed_idle only after authenticated confirmation that both running and queued budgets are zero, accepted dependency closure is empty, and the admission barrier remains closed. A later nonzero observation clears idle proof and blocks stop. Generic idle heartbeat alone supplies no barrier proof.

Create/claim stop only after that durable proof. Drain timeout, lost reachability or controller restart cannot manufacture it: leave draining/stalled, then revalidate with the worker before stop. An already terminated provider record resolves a lost worker without initiating unsafe termination. There is no forced stop for an unreachable possibly busy resource. This admission/drain guarantee does not claim to revoke an already submitted database CAS; C02's conditional publication and reachable request decisions still decide its outcome.

Adopted [A21](DN-A21.md) pins concrete drain/stop transitions and receipts.
Worker proof-heartbeat acknowledgement and owner settlement/reaping are separate
requirements. Its narrow memory-only observer exception grants no shutdown
exception. Controller proof continues only through checked complete-root idle
confirmation, first-stop allocation and exact delivery claim; it retains the
original round limits, and any other newer root requires fresh evidence. The
selected C06 stop flow requires registered exact-incarnation drain proof; a
wire-permitted null incarnation alone grants no stop eligibility. Terminal
reconciliation uses unique exact positive terminated inventory, not stop-null
or ordinary absence. Any additional authoritative proof requires its own
explicit adopted design. These remain implementation obligations.

## Admin ledger independent of root revisions

`control-decisions/<id>.json` uses immutable Create only. The exact admin record is:

```text
{version:1, id:UUID, parent:UUID|null, admin_seq:positive_u64,
 cluster_id:UUID, base_root_revision:positive_u64,
 published_root_revision:positive_u64,
 request_id:UUID, request_body:string,
 response_status:100..599, response_body:string}
```

Request and response strings contain their exact UTF-8 envelope bytes, each at most 1 MiB. Bound encoded decisions at `4 MiB + 64 KiB` for JSON quoting. Validate the original admin request/envelope/UUID and matching precomputed terminal response, never log the bodies. Store scale/autoscale successes, identical-policy successes and deterministic validated policy errors, including unsupported external autoscale; no status-read, auth/framing, overload, storage or unknown-outcome decisions. Authorization always precedes replay lookup. Request UUID plus exact body bytes binds identity in this cluster namespace; whitespace changes under the same ID conflict.

Parent codec reconciliation: the request is the existing strict
`{request_id,operation,payload}` envelope. Only `cluster.scale` with `{nodes}`
and `cluster.autoscale` with `{min_nodes,max_nodes}` enter this ledger; count
fields use exact u64 tokens. A structurally valid typed request outside policy
bounds may record only `validation_error`/400, never success. Malformed shapes,
types and unsupported operations remain outside terminal history. Success is
HTTP 200 with the exact correlated `{request_id,data}` envelope, where data is
the five-field policy accepted by the SDK. Fixed acceptance matches the requested
target and equal bounds; autoscale acceptance matches requested bounds with
desired inside them. A positive policy revision is independent of the root/admin
sequence. Terminal errors require the same non-null request UUID, strict string
code/message/outcome fields, and outcome `not_applied`. The codec allowlist is
`validation_error`/400, `unsupported_capability`/400, `conflict`/409 and
`limit_exceeded`/413. For a request outside policy bounds only the first applies.
The caller still owns producing the correct deterministic policy outcome and
sanitized message. Codec validation does not infer authorization, external mode,
reachability or that a decision was committed.

When admin_seq=0, head is null. Otherwise head names a decision with that exact sequence and cluster. Each parent has sequence exactly one lower; sequence 1 alone has parent null. Each record has published_root_revision=base_root_revision+1; following parents only requires their published_root_revision <= child's base_root_revision. The loaded root revision must be >= its head's published_root_revision. Do not require head==change_id or consecutive root revisions across decisions. Non-admin writes preserve head/sequence exactly.

After complete absence lookup, prepare response bytes and immutable record, then CAS the policy and `(admin_seq+1, admin_head=id)` into the same root, with root revision+1/change_id fresh. An identical normalized policy preserves policy.revision, but still adds its durable request outcome and advances root/admin revisions. Fixed scale disables autoscale and sets both bounds to target; autoscale replaces bounds and clamps current desired. A change of effective policy increments policy.revision. Controller autoscale target changes within the same bounds do not increment it, so Python waits observe policy supersession rather than controller housekeeping. Status/acceptance expose policy.revision, never root.revision. Replay returns the original stored response even if later policies differ.

CAS losers reload and search first, then recompute. Use a separate control-history walker, reusing storage/key/budget techniques from `storage/history.rs`, not its catalog head/commit or revision invariant. Match exact body for Found, return identity conflict for a different body, and prove absence only at a validated sequence-zero terminus. Per lookup, recommend 4096 records, 32 MiB aggregate reads, and a five-second monotonic deadline bounded by the caller's remaining budget. Missing/corrupt/unavailable history or exhausted budget is unavailable/unknown, never absence. No TTL/GC or forgotten request IDs; linear lookup is an explicit availability/throughput limit.

Lost CAS replies search the latest root's reachable admin ancestry, including after renewals/membership changes and later admin calls. Orphan immutable records prove nothing. If not found, a still-pending original PUT may yet win. Return unknown unless a new complete-lookup/candidate CAS from a fresh root establishes a terminal outcome and fences the earlier token. Never blindly repeat the PUT or issue a new request UUID internally. Non-admin updates cannot sever the ledger.

## Small transition boundary and implementation order

Use plain records and functions. Pure functions receive validated snapshots, typed observations, explicit wall/monotonic times and caller authority; they return a candidate plus bounded planned effects or a sanitized error. They do no I/O. Keep a single orchestration owner for each side effect:

```text
initialize_or_attach(config, initial_policy) -> LoadedControl
load_control() -> LoadedControl
transition(root, authority, event, times) -> Candidate
cas_control(loaded, candidate) -> Applied | Conflict | Unknown
lookup_admin(loaded, request_id, exact_bytes, budget) -> Found | Conflict | Absent
plan_reconcile(root, inventory, observations, times) -> bounded Events
authorize_callback(exact_command_bytes, local_lease) -> A04 authorization reply
project_status(root, observations) -> existing bounded status envelope
```

Events need only acquire/renew/release, accept policy, register/verify/expire, begin/ack drain, allocate/claim/resolve intent and set autoscale target. Separate request-history publication from the pure policy calculation. The controller owns storage transitions and delivery claims; the callback client owns one bounded HTTP attempt; Python owns callback slots; the worker owns admission/barrier evidence. Effects execute only after confirmed CAS and fresh private authorization. Defaults, backoff and sample thresholds are constants/configuration, not a provider/actor/consensus framework.

Managed provisioning counts the union of nonterminated members, unresolved launches and positive matching resources by launch/node identity, including stalled ambiguous starts and readiness lag. Count ready separately, never `ready + all intents`. Respect desired/bounds against committed reservations; drainage/old resources consume physical overlap even after they stop serving. Serialize new reservations through CAS, cap physical tracked membership at 2048, and do not replace unresolved resources. External fixed scale persists only an operator goal; provision/terminate/autoscale capabilities are false. Excess external workers may drain, never receive provider termination.

For autoscale, grow desired by one when all eligible workers have fresh observations and any operation/partition queue remains nonzero, subject to bounds/cooldown/backoff and existing launch reservations. Shrink by one only after every nonterminated member has continuously fresh zero running/queued work across stabilization, no unresolved intents or drain, and authenticated drain remains possible. Reset local low-load evidence on takeover, gaps or policy changes; persisted low_since alone never proves continuity. Fixed overrides invalidate old autoscale proposals through their loaded token/policy revision. Reserve before launch, persist target/cooldown before action, and preserve stalled intents/backoff across all policy changes.

Parent should issue these MINI assignments sequentially at shared wiring points. Each is a small Sol-high task, followed by the parent's existing specification/quality review workflow. Likely ownership and focused evidence:

1. Records/codecs: new `server/src/cluster/{mod,state}.rs`, `server/src/storage/control_decision.rs`, `server/tests/control_records.rs`; narrow exports in `server/src/{lib,storage}.rs`. Cover a representative 1024-node candidate, reserved growth/encoded overflow, strict required-null/canonical identity rejection, and command digest reconstruction with distinct local credential paths. No runtime wiring.
2. Root CAS/controller ownership: new `server/src/cluster/{store,controller}.rs`, `server/tests/control_cas.rs`. First C04b1 owns only bounded validated loading and single-attempt initial creation in store.rs, with tests/control_store.rs. Its immutable LoadedControl retains the private nonempty backend token and redacts it from Debug. Creation validates an exact initial-state candidate before I/O, GETs first, sends at most one Create after definite absence, and on Conflict/Ambiguous or an empty success token performs at most one fresh GET under the original absolute deadline. A valid observed root is returned unchanged, including a competitor's root; it is not proof that this candidate won or that attachment is compatible. Existing roots are never overwritten or compared to a new serve policy. Failed reconciliation after a possibly applied Create remains Ambiguous, and a timed-out possibly submitted Create is never resent. Runtime attachment must still verify identity and semantic configuration before use. C04b2 and later small assignments then add owner-checked transitions and root CAS. Gate stale completion/renewal against takeover; lose acquisition/relinquishment replies and require fresh CAS before authorization; cover exhaustion without identity reset. Parent owns startup wiring in `main.rs`/`runtime.rs` after these slices.
3. Admin decisions: new `server/src/cluster/admin.rs`, `server/tests/control_admin.rs`; parent owns separate G02 routing integration in `api.rs`. Lose accepted response, interleave renew/member writes and another admin decision, then recover identical bytes; race same-ID different bodies; prove identical policy still records an outcome and incomplete history never executes. Reuse storage fakes/barriers, no catalog walker invariant copying.
4. Membership/heartbeat and authorization: new `server/src/cluster/{membership,heartbeat,authorization}.rs`, `server/tests/control_membership.rs`, `server/tests/control_authorization.rs`. Split these into two MINI tasks if wiring exceeds one review. Cover takeover observation grace, old-sequence rejection after drain/reboot, lost registration reply/session recovery, and readiness blocked until authenticated resource verification. Gate authorization while callback I/O is pending to prove independent progress; stale owner/hash/target or lease uncertainty must deny entry. Detailed HTTP session/auth transport stays with R03/R04/R07 startup assignments.
5. Provision/drain/autoscale: separate MINI tasks owning new `server/src/cluster/{provision,drain,autoscale}.rs` respectively, with matching `server/tests/control_{provision,drain,autoscale}.rs`. Gate provider acceptance then lose reply/take over and discover the same late resource; absence/duplicate inventory cannot produce a replacement. Hold accepted work through drainage and lose stop reply, then recover exact termination; stale metrics/backoff/fixed override must block an old shrink/grow plan. Parent serializes controller integration and shared exports. Reuse E03/E04 actual-process acceptance already planned in A03; create no second runner/framework.

The parent selected the local clock-discontinuity tolerance above and adopted A07's corrected session/HTTP contract after independent review. No new public configuration argument is needed. Treat lifecycle archive/compaction and credential rotation as later explicit designs; retain records and fail admission safely now. These choices do not weaken fresh authorization, durable request identity or the drain barrier above.
