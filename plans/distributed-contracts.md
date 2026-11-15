# Distributed-node implementation contracts

Implement the public interface agreed in [the rubric](distributed-nodes.md), using the existing Rust engine and Python package. This file resolves A01-A03 differences and is authoritative for implementation assignments. Historical notes remain design evidence, not competing specifications.

## Roles and ownership

The Python `serve` supervisor runs the user's infrastructure callbacks and starts one Rust API process. The Rust API handles client/admin HTTP, shared membership, routing and controller reconciliation. It performs no partition execution. Any ready Rust worker accepts a whole operation, coordinates typed partition jobs across ready peers including itself, and performs its own admitted partition jobs. All workers use the same code. Shared conditional storage is the authority; no permanently designated worker coordinates all writes.

Operation permits and partition permits are separate. Coordinating work never holds a partition permit while waiting for partition jobs. Partition handlers never acquire an operation permit or recursively dispatch. Bounded coordinator CPU work is independent of the partition pool. Heartbeat, readiness and drain handling remain runnable when both compute budgets are full. A draining coordinator can finish self-dispatched dependencies for its already accepted operations while rejecting newly arriving operations/jobs.

Default operation/partition capacities are 4 each; each accepts an exact integer in 1..256. Queues are separate and bounded, each using `queue_size` in 0..4096. No worker-count setting limits a machine to one query. Capacity claims in membership are hints; each worker enforces admission locally. Result, scratch and queue budgets apply per process/operation, not multiplied by an unbounded fleet.

## Persistence and publication

Use one configured conditional-object backend for immutable table objects, catalog, decision history and cluster control keys. Core requires linearizable GET, immutable Create and atomic Match with durable non-ABA version tokens. Provider clients and credentials remain in adapters. Split AWS-specific implementation/dependencies behind the `s3` Cargo feature; the non-AWS runtime must build and operate without it. Preserve the legacy S3 entry point when that feature is enabled.

Replace generic `Download.body`'s AWS type with a provider-neutral asynchronous reader; preserve read limits while consuming chunks. A constructor from bytes supports existing test fakes. Prefer existing Tokio reader facilities over a second abstraction framework. New typed metadata keys explicitly allow decisions/control/heartbeats; arbitrary snapshot overwrites remain forbidden.

Canonical metadata paths are `decisions/<uuid>.json`, `control-decisions/<uuid>.json`, `cluster/control.json`, and `cluster/heartbeats/<incarnation-uuid>.json`. Decision objects are immutable Create-only. Catalog, control root and per-incarnation heartbeats permit conditional replacement. UUID spellings must be canonical lowercase; no arbitrary metadata paths are admitted.

Local development/acceptance uses `scripts/local_store.py` as specified in the rubric. It exposes SQLite-backed conditional objects over HTTP and is a single persistence availability domain. Production HA is supplied by a conforming backend, not claimed for this local helper. HTTP/S3 adapters have the same conformance requirements.

Adopt A02's catalog-v3 linked immutable decision history. A successful mutation atomically publishes its table changes and the decision head in one catalog CAS. Record no-op successes and deterministic mutation errors with an unchanged table set. Scope mutation identity to the durable namespace and exact original request bytes, including its UUID. Authorize before replay lookup. Reads keep fresh-read behavior. Complete original response bytes/status are prepared before publication and replay unchanged; malformed framing/auth and transient failures are not terminal decisions.

CAS losers reload history and recompute from the new catalog, with a bounded contention retry budget. This establishes a serial commit order without a distributed process mutex. An ambiguous publication searches reachable history, including after superseding commits; an orphan decision is not evidence. Incomplete lookup returns a safe availability/unknown outcome and never authorizes blind reexecution. Do not independently promise that heartbeat expiry revoked a pending database CAS. A stale attempt is excluded by the root token and same-request decision checks.

Retain decision history without TTL/GC in this version. Linear lookup is a documented throughput limit; the implementation must not silently forget identities or treat a bounded/incomplete traversal as absence. Migration preserves all v1/v2 table identities/manifests and requires the old standalone server to be stopped. Cluster startup performs the one-way conditional v3 initialization/upgrade; old writers must not coexist. Existing standalone v1/v2 behavior stays available outside cluster mode.

Catalog-v3 adds required nullable `decision_head` to the existing root fields and keeps the 1 MiB encoded limit. Decision v1 records have `version`, `id`, nullable `parent`, `base_revision`, `revision`, `request_id`, `request_body`, `response_status`, and `response_body`. The bodies are exact UTF-8 strings, including whitespace and numeric spelling, each at most 8 MiB. Bound the encoded decision at `4 * MAX_BODY_BYTES + 64 KiB`: JSON string quoting can double each valid original JSON body, so A02's illustrative two-body encoded allowance would reject otherwise valid requests. This bound fits the local backend and does not permit larger public bodies. Decisions accept mutation requests and precomputed terminal success or deterministic operation errors only; authentication, overload, storage, internal and unknown-outcome failures are not terminal history. The decision revision is exactly base revision plus one; atomic publication later checks its ID/parent against the candidate/current root.

The separate bounded control root contains persisted initial configuration, policy revision, desired count, controller owner/epoch/lease, node launches/incarnations and lifecycle intents. Mutating admin calls also have durable request outcomes. Heartbeats use per-incarnation keys. Only a successful control-root CAS grants takeover authority; every controller update checks its owner/epoch against the loaded root. Callback effects may finish late, so takeover adopts the same persisted command identities and reconciles inventory before replacement.

Adopt [A05's exact control records and transitions](../server/notes/DN-A05.md),
including its parent reconciliation and [A08's capacity correction](../server/notes/DN-A08.md).
Accepted [A10](../server/notes/DN-A10.md) pins ownership-only CAS and local
certainty. Guard admission and write preparation share one explicit proposal
UUID and complete expected candidate. Read-back can confirm metadata but never
install live authority. A newer root with an unchanged complete controller
tuple preserves installed certainty and its original deadline; it never restores
or extends it. Late completion/error/cancellation is scoped to its original
stamp, and fresh callback authorization rejects a superseded root observation.
The control root is bounded to 32 MiB with maximum-field growth and byte/count
reservation for each eligible managed node's first future stop. New admissions
preserve those reservations; first-stop conversion and updates of retained
identities use the reserved room. Public status stays at 1 MiB.
Root revision/change UUID, policy revision and admin-history sequence are
separate. Internal updates preserve the admin chain and policy revision.
Lifecycle records remain retained until an explicit future archive exists;
metadata exhaustion refuses unreserved allocations without forgetting ambiguous
effects. Stop requires an authenticated drain barrier for the exact incarnation;
timeout or an idle heartbeat alone cannot authorize termination.

## Python interface and callbacks

```python
serve(name: str, nodes: int | tuple[int, int] = 1,
      start_node=None, stop_node=None, list_nodes=None) -> None

Cluster(api_url: str, admin_token: str, *, timeout: float = 30.0,
        ca_file: str | None = None)
Cluster.scale(nodes: int, *, wait: bool = False, timeout: float = 300.0) -> dict
Cluster.autoscale(*, min_nodes: int, max_nodes: int) -> dict
Cluster.status() -> dict
```

Validate names using the current identifier rule. Counts are exact integers excluding bool. Fixed counts permit 0..1024; zero means intentional drainage to zero. Autoscale requires `1 <= min_nodes <= max_nodes <= 1024`; waking from zero is outside the agreed interface and is not added. A node tuple must be exactly a two-element tuple. Callback sets are all three synchronous callables or all absent. Reject coroutine callbacks/awaitable results. Imports and construction have no side effects.

`serve` blocks until orderly shutdown. Initial `nodes` applies only when atomically creating the cluster; later API replicas attach without resetting persisted policy. Fixed `scale` disables autoscaling. `autoscale` replaces bounds and clamps the existing target. Identical policy submissions preserve their revision. Return `{revision, mode, desired, min_nodes, max_nodes}` after persistence. With `wait=True`, require that revision to remain current, ready count to equal desired and no incomplete lifecycle intents. Supersession raises `ConflictError(code="policy_superseded")`. The overall monotonic timeout starts before submission; expiration after acceptance raises `TransportError(code="wait_timeout")` and does not cancel policy. An unacknowledged possible write is `UnknownOutcomeError`. No hidden Python mutation retries.

Callbacks use the plain dictionary shapes in A03. `start_node` receives version 1, cluster UUID/name, stable provisioning request UUID, logical node UUID and bootstrap dictionary. The callback persists those identities with the resource and returns a nonempty opaque resource ID, at most 256 UTF-8 bytes with no control characters. Repeated starts for one request must converge on the same resource across crashes. `stop_node(resource_id)` is idempotent and targets that exact resource; resource IDs cannot be recycled for a replacement incarnation. `list_nodes()` returns complete cluster-owned inventory records `{resource_id, cluster_id, node_id, request_id, state}` with state starting/running/stopping/terminated. Positive records reconcile lost start responses. Missing inventory alone never proves an ambiguous launch failed. Unknown/duplicate resources are quarantined, not blindly stopped.

The callback bridge is loopback-only, authenticated, bounded to four active callbacks including an inventory reservation, and verifies persisted command ownership. A timed-out callback is ambiguous; do not assume its thread or side effect stopped, grow replacement threads without bounds or issue a new launch identity. Saturation degrades provisioning while data traffic continues. Worker credentials/bootstrap are never exposed in status or logs.

Use [A04's private startup/authorization protocol](../orm/notes/DN-A04.md): a
16 KiB bounded framed inherited socket carries init/prepared/activate/ready and
lifetime shutdown, with only the descriptor number passed through the
environment. Rust exposes an independent authenticated loopback authorization
endpoint that freshly checks persisted owner/epoch/intent before Python invokes
a callback. Exact command bytes are bound by SHA256; authorization is not atomic
fencing of a later provider effect. The bridge has three action threads, one
reserved inventory thread and bounded ingress. Parent simplification supersedes
A04's proposed completed-result cache: retain only active-call identities and
their timeout ownership. Completed calls leave no Python result cache; later
attempts require fresh authorization and provider idempotency. A hung call keeps
its slot, and a surviving callback pool blocks a second serve runtime in that
process. Callback code owns its own logging and any threads/subprocesses it
creates; the SDK sanitizes only its own protocol/errors.

Adopt [A19's managed provisioning foundation](../server/notes/DN-A19.md).
Start commands reconstruct the persisted Template and retained enrollment
material before hashing; only digests and delivery identities enter Control.
Unresolved launches retain capacity and first-stop credits through timeout,
takeover and absent inventory. Exact positive evidence recovers the same launch.
Without richer retained delivery history, definitive pre-entry failure may
terminalize only the first exact current attempt under A19's additional checks;
later ambiguous attempts remain reconciling with persisted capped backoff.
Inventory absence and stop-callback success establish no termination. Actual
allocation gates, fresh callback authorization, supervised delivery and C06's
positive drain/termination proof remain required beyond these candidate helpers.

Adopt [A21's corrected drain, stop and termination handoff](../server/notes/DN-A21.md).
Positive drain evidence binds the exact initialized boot, permanent barrier,
retained-work settlement and a barrier-time acknowledged heartbeat whose own
publication obligations have settled and reaped. Only the current memory-only
drain observer and its response transport may be excluded from work-idle; their
ownership remains retained, and whole-process shutdown has no such exception.
Proof-only heartbeat lifecycle changes do not invalidate work-generation evidence,
but actual remaining owners always block a positive result. Confirmed proof can
continue through only checked exact idle-confirmation, stop-allocation and
stop-claim successors, preserving its original round limits. Other newer roots
require fresh authenticated evidence. Reserved first-stop conversion preserves
A08 capacity guarantees. Stop acceptance is not termination; irreversible
resolution requires unique exact positive terminated inventory under the existing
provider contract. These are pending implementation and acceptance obligations.

Adopt [A23's corrected autoscaling handoff](../server/notes/DN-A23.md), including
all five parent refinements. Grow only from a nonempty fresh eligible fleet when
every charged managed member is eligible, physical managed capacity equals desired,
and no unresolved intent or drain remains. Shrink requires the same physical
equality, continuously fresh zero work from every nonterminated member, and a
managed structural drain target. Exact retained terminal witnesses govern
exclusions; structural feasibility grants no authenticated idle or stop authority.
Empty or deficient fleets use the existing persisted target and provisioning path.
Identical heartbeat repeats preserve valid low time without extending expiry or
restoring invalidated evidence. Continuity covers accepted observations only.
Ordinary ownership renewal suspends eligibility through the explicit existing-guard
stamp/generation handoff; only matching acknowledged completion, a fresh same-owner
check and progressing heartbeats within retained deadlines permit resumption.
Failure, invalidation, takeover or expiry resets local evidence. A new lease root
alone cannot restore it. Target/cooldown persistence precedes lifecycle actions,
preserves policy/admin history and retained failures, and supplies no live authority.
The bounded metrics, pure candidates and consumed submission remain separate
implementation assignments; this adoption establishes no runtime scaling result.

With callbacks absent, use external mode: manually bootstrapped workers can join, fixed `scale` changes an operator goal, and status reports provisioning/termination/autoscale unavailable. Reject autoscale and tuple initial nodes. Operators own external machine termination. Replacing machines does not require moving committed partitions.

## Configuration and commands

Use `MESHDB_CONFIG` for a versioned JSON API configuration and optional `MESHDB_SERVER_BINARY` to locate the built Rust binary. Keep secrets in the referenced protected auth file. Config top-level fields are `version`, `cluster_id`, `bind`, `advertise_url`, `api_urls`, `storage`, `auth_file`, `tls`, `worker`, and optional `lifecycle`. This replaces A03's separate `control`/`objects` fields with one backend binding for this release.

`storage` is `{driver, options}`. First runtime drivers are `http` and feature-gated `s3`; fail unknown drivers explicitly. HTTP options supply base URL, namespace, token file and optional CA file. S3 options supply bucket/namespace and use its standard credential chain. TLS is null or `{cert_file,key_file,ca_file}`. The auth file contains distinct admin/data/enrollment secrets. Worker defaults include listen address, operation_slots, partition_slots, queue_size and partition_size. Lifecycle overrides exist for bounded deployment/testing timings, with validated positive heartbeat/expiry/readiness/deadline and stabilization values. Never require AWS fields for HTTP storage.

Adopt [A11's corrected configuration and attachment contract](../server/notes/DN-A11.md).
The private control Config uses generation 2 and adds required
`worker_semantic_fingerprint`; the control root and public input versions remain
1. The exact outgoing-template fingerprint remains unchanged and is verified
against the stored template. Replica attachment separately compares loaded
credential/trust identities, effective backend identity and worker settings,
permitting equivalent material at different local paths. Use the original
persisted template for callbacks and never open its foreign paths as local
configuration. Loaded bytes used for identity are also used for runtime setup.
Generation-1 control configurations fail incompatible without writes; no
automatic upgrade or mixed-generation rollout is supported. Standalone catalog
and serve behavior are unchanged. The C08c assignment owns the coordinated
codec/fixture amendment; its pending implementation is not implied by adoption.

A11's bounded material loading allows 64 certificates per leaf chain and 1024
per CA bundle, including system bundles, while preserving its byte limits.
Before C08b, the exclusive Cargo owner adds the already locked/cached
`libc = "=0.2.191"` as a direct Unix dependency, independent of S3. The loader
uses nonblocking Unix opens and checks the opened handle before reading, so a
regular-file-to-FIFO swap cannot create an unbounded open. No dependency fetch,
credential rotation, new public setting or PKI is introduced.

Adopt [A14's corrected identity and backend composition handoff](../server/notes/DN-A14.md).
The fixed fingerprints reuse the bounded canonical writer and retained local
material. HTTP construction uses that same effective token and trust material.
Distributed S3 startup rejects raw configured endpoint overrides, rejects a
loaded shared endpoint, and resolves the exact S3 endpoint service key once.
Both clients use a private provider wrapper retaining the accepted absence for
that key and forwarding other settings unchanged. The standard-policy marker
belongs only to clients constructed through that decision; a later live lookup
cannot replace it. This retains no general atomic snapshot of AWS sources.
Optional pinned aws-types 1.7.0 and aws-runtime 1.10.3 are permitted under S3,
with isolated aws-config test-util support behind a dedicated test feature.
No AWS dependency enters no-default builds and legacy S3 startup is unchanged.
Implementation and feature verification remain C08b2a/b/c work.

The API child command is `meshdb-server api --config PATH`; its supervisor passes callback-bridge address/secret privately. Public worker command is `meshdb worker --bootstrap PATH`, forwarding directly to `meshdb-server worker --bootstrap PATH` without a shell or Python execution engine. Preserve `meshdb-server serve` for standalone mode. No automatic executable download or cloud provisioning at import.

Executable resolution uses an explicit `MESHDB_SERVER_BINARY` path when set, otherwise `meshdb-server` on PATH. An invalid explicit override fails rather than falling back. The worker wrapper replaces itself with the Rust process, preserving signal ownership and exit status; Rust owns bootstrap-file validation. Help and imports do not resolve or start a binary.

Worker bootstrap follows A03, replacing control/objects with storage. It includes version, cluster/node/request identities, api_urls, listen and advertise URL, storage, enrollment_token, TLS and capacity/partition settings. Provisioning receives advertise_url null; the adapter fills the target address and target-local credential paths without changing cluster/request identity or capacities. The worker binds, validates storage/catalog, and registers a fresh boot incarnation. Managed registration must match a persisted launch; external registration has request_id null. Ready requires matched resource identity, registered incarnation and reachable authenticated execution endpoints. Distinct scoped worker sessions protect internal routes.

Adopt [A07's corrected enrollment and internal HTTP contract](../server/notes/DN-A07.md).
Per-boot Ed25519 keys and public-key-derived session verifiers replace A05's
original secret-hash verifier; a separate API key derived from the existing
admin secret replaces its shared enrollment-derived message key. No new public
secret field is added. Signed scopes bind exact content, current identities and
bounded replay state. Enrollment reaches API listeners and can recover retained
late launches or an exact draining binding without reviving terminal identities.
Operation exchanges cannot consume reserved dependency-job/control transport.
Pre-barrier dependency permits retain accepted work through nonblocking close;
idle proof includes publication, delivery and cleanup owners. HTTP body adapters
must reject trailers and preserve real body errors/EOF. A07's explicit trusted
member/backend and local-HTTP limits apply; adoption is not runtime test evidence.

Adopt [A15's corrected authentication implementation handoff](../server/notes/DN-A15.md).
Pure wire, credential verification and replay helpers remain separate from
fresh durable authority, HTTP and lifecycle transitions. Initial body/CONTROL
validation retains bounded transport admission under supervised ownership and
runs finite hashing/parsing commands on the coordinator CPU/I/O pool. Cancellation
settles submitted work before releasing admission. Final admission performs only
bounded checks, consumes replay state atomically with reservation, and transfers
memory-only checked CONTROL exactly once with the same FrameReader and preserved
body errors/EOF into registered payload ownership. These integration obligations
remain unimplemented by the design note. The approved cached ring dependency and
fixed-domain signing keys are implemented and reviewed under R03a2a.

Adopt [A17's signature, content and membership handoff](../server/notes/DN-A17.md).
Opaque proofs retain exact invocation associations; enrollment possession alone
grants no membership or readiness. Response contexts retain the full original
request and durable target verifier, with unique per-target request sequences.
Content hashes cover original bytes before typed parsing. Membership eligibility
retains an exact complete-root observation and pending local checks; fresh reads,
root ordering, replay, capacity and dependency admission remain later integration
obligations. Null-ID operation rejection never becomes a terminal DB outcome.

Adopt [A18's corrected sequence, replay and consuming handoff contract](../server/notes/DN-A18.md).
One boot-owned sender counter issues increasing sequences across routes and
targets, with checked exhaustion and complete identity/key binding. The API
constructor checks the key's retained cluster through a read-only accessor.
Receiver replay retains bounded non-evicted sender evidence and complete-root
ordering; an integrity mismatch fails that receiver boot closed. Consuming
request/reply handoffs preserve exact invocation associations and original
checked bytes. These pure helpers do not reserve real capacity or transfer
supervisor ownership. Final live checks, reservation, replay consumption and
infallible accepted-owner installation share the actual admission mutex;
supervised prelude settlement and the same advanced FrameReader remain required.

Adopt [A20's corrected worker admission and retained ownership contract](../server/notes/DN-A20.md).
An admission-owned durable view retains the target phase/drain facts from the
same validated root and eligible proof. One receiver mutex orders that view,
permanent barrier closure, actual capacity/dependency reservations, replay and
accepted-owner installation. Preparations enter the registry before their tasks
can be abandoned; rejected owners retain handles and charges through reaping.
The 32 preparation positions reserve eight for controls and 24 for data/boot.
Process initialization has one boot-bound path and a named R07 completion seam;
controls reserve no compute slot. Transaction rollback uses the current mutable
state without recursive locking. Queue start rechecks current authority, while
closing permits retain already accepted uses. Initialization, unresolved work,
publication, cleanup and unreaped handles remain part of positive-idle checks.
These are implementation obligations, not proof that runtime admission or drain
is already implemented.

Adopt [A22's corrected final-admission and drain-observation handoff](../server/notes/DN-A22.md).
Checked durable and accepted-request authority views retain the original binding
and exact complete-root/boot association. Configured initialization uses an opaque
R07-issued capability together with the actual retained process-initialization
receipt; no caller ready flag substitutes for either.

Atomic acceptance reserves immutable authority storage and a separate one-use
owner payload position before replay consumption. Commit consumes the original
handoff into those positions without allocation. Start subjects retain authority
only, so the original registered owner can move the checked bytes/body without
Arc uniqueness, copying or reparsing. Transfer remains owned through accepted
command settlement, cleanup and actual reaping.

One fixed boot-bound proof-heartbeat slot retains the single writer, task and
result through settlement and reaping. A timely acknowledgement alone cannot
release that ownership or grant idle. Only the exact proof-only cycle is excluded
from work-evidence generation; the final same-lock check still inspects all
retained owners. The exact authenticated drain observer may be excluded only
from work-idle, never whole-process shutdown. Unknown provider effects remain
retained and block positive idle. These are bounded implementation assignments;
HTTP, runtime composition and distributed acceptance remain required.

## Administration and status wire records

Use authenticated `POST /v1/cluster/operations`, with existing request/response envelope shapes and operations `cluster.scale`, `cluster.autoscale`, `cluster.status`. Data remains `POST /v1/operations`. Distinct admin/data credentials authorize only their own routes; enrollment/session credentials are separate. Bound control envelopes at 1 MiB, including responses, with strict object-only JSON, duplicate-field rejection, exact numbers and no unknown fields. Use existing status/error categories; `unsupported_capability` is a normal 400 MeshDBError. Never forward client tokens to workers/callbacks.

Adopt [A13's corrected administration-history/publication contract](../server/notes/DN-A13.md).
Any attached authenticated API replica may publish policy using the captured
control token while preserving the controller tuple. Authorization and reachable
history lookup precede candidate preparation and counter arithmetic, including
when publication counters are exhausted. Only complete absence permits a new
candidate. Publication does not repeat that lookup: the initial scan and optional
reconciliation scan share the absolute budget created before initial root loading.
Each scan is capped at 4096 records and 32 MiB of decision bytes; the separately
bounded root read does not consume the decision-byte allowance. Immutable record
confirmation and one root Match retain exact request/response bytes. Uncertain
writes remain uncertain until a reachable terminal decision or an explicitly
controlled fresh CAS resolves them; an orphan record or expired deadline is no
proof. These assignments grant no provider authority and add no public facade.

Status is A03's exact version-1 dictionary: cluster_id/name/revision/mode/desired/min_nodes/max_nodes; capabilities provision/terminate/autoscale; controller epoch/active/degraded; counts ready/starting/draining/lost; nodes; intents; metrics_fresh. Node records have node_id, nullable incarnation/resource_id, ownership managed/external, state starting/ready/draining/lost/stopped, operation_slots, partition_slots, running/queued operations/partitions and nullable finite heartbeat_age_seconds. Intent records have request_id/node_id/nullable resource_id, action start/stop, state pending/in_progress/reconciling/completed/failed, attempts and nullable sanitized error_code. Sort by identity, cap combined records at 4096 and never silently truncate. Retain durable outcomes separately when compacting completed control records.

Intent error codes are open machine tokens: null or 1..256 ASCII letters, digits, underscore, dot, colon or hyphen. Provider messages and secrets must never be copied into this field. Node resource IDs are unique within membership; retained lifecycle intents may reference the same node/resource.

## Execution transport and validation

Adopt A01's pure Get/Collect/Count/Prepare typed jobs and explicit result identity. Envelopes carry version, cluster/operation/job/attempt IDs, original request digest, captured catalog identity, ordinal, descriptor and task. Inputs are captured file references; jobs never read a new catalog to reinterpret them. The handler reuses existing Parquet validation, mutation rules and local query evaluation. Only the coordinating operation publishes a catalog. Final ordering/limit/projection happens once; count sums before global limit.

Accepted [A12](../server/notes/DN-A12.md) pins the captured handler and its
registered worker-side output handoff. Extract one shared incremental partition
emitter while retaining the existing collecting wrapper. Locally produced
prepared files use the same actual-file verification and bounded metadata rules
as received files. Register empty disposal cells before partition work; only
execution threads create or finally destroy disk owners. Local completion uses
its own validated transition, while framed completion still requires actual EOF.
Typed metadata cannot establish historical catalog provenance or detect omitted
peer matches; trusted captured context and the complete-input handler supply
those responsibilities. No new public API or object publication belongs to this
handler slice. Its pending implementation is tracked separately from adoption.

Internal HTTP uses authenticated length-prefixed control headers and bounded raw chunks, rather than base64 or JSON byte arrays. Keep control headers at most 16 MiB and chunks at most 64 KiB; enforce bounds before allocation. Internal intermediate rows may exceed the public 8 MiB final response cap. Enforce existing source-row/decoded-byte limits and A01's conservative escaped-JSON bound; spool/bounded readers avoid allocating the entire network maximum. Each closed Parquet replacement remains at most 64 MiB, legacy reads at most 80 MiB. Bound aggregate temporary storage and fail before publication on exhaustion. Preserve the existing rule that all preparation/validation succeeds before table-object uploads. The exact framing implementation is a separate small assignment after DTOs.

The initial result CONTROL never embeds a Get row or the full replacement list. Get carries source counts and `found: bool`; its subsequent row stream is a JSON array with exactly zero or one row, using the same bounded chunks and escaped-row allowance as Collect. DTO validation of the decoded row preserves schema, requested key and source-payload accounting without applying the CONTROL serialization cap. Prepared carries the effect and nullable `replacement_count`: null means unchanged, zero means changed to empty. Each replacement then has one separately bounded PartMeta record followed by its Parquet stream. An incremental metadata validator checks the declared count, contiguous ordered ranges and checked aggregate rows/payload/encoded bytes without collecting all metadata into CONTROL. Permit up to 110,000 pre-finalization replacement rows/files, then enforce the existing global table and catalog limits before any upload. The header, each PartMeta record and raw data chunks keep their separate bounds. These wire representations replace A01's illustrative embedded values without lowering its data limits.

Frame v1 has a one-byte kind and four-byte unsigned big-endian payload length.
Kinds are CONTROL=1, DATA=2, PART_META=3, PAYLOAD_END=4 and STREAM_END=5.
CONTROL/PART_META are nonempty and at most 16 MiB; DATA is nonempty and at most
64 KiB; terminators have zero length. Reject unknown kinds, invalid lengths and
truncation before treating an attempt as complete. Bound the length before any
payload allocation. The initial CONTROL contains the existing versioned DTO.
Requests consist of CONTROL then STREAM_END. Count/error replies have that same
shape. Get/Collect replies add one JSON-array payload (DATA chunks then
PAYLOAD_END); missing Get rows and empty Collect results still encode `[]`.
Prepared replies add exactly the declared number of PART_META records, each
followed by its Parquet DATA chunks and PAYLOAD_END. Null/zero replacements have
none. Every stream ends with STREAM_END and actual body EOF; trailing bytes are
invalid. No partial stream becomes success. Transport cancellation abandons the
attempt; a reader/writer whose operation was cancelled mid-frame must not be
silently reused. HTTP authentication/deadlines and semantic payload completion
remain separate from the small low-level frame codec.

Adopt [A06's semantic payload and executor boundary](../server/notes/DN-A06.md),
including its parent reconciliation. Row arrays use an immutable bounded spool,
a lexical resource preflight and strict seeded decoding; no complete encoded
array or raw escaped scalar-string copy is allocated. Internal numeric tokens
are at most 128 bytes, preserving current i64/f64 serialization and signed zero;
this restriction does not change public request parsing. Prepared files verify
actual length and SHA256 before existing Parquet/schema/range checks. Use the
cached direct sha2 0.11.1 dependency. Synchronous disk/decoding/cleanup commands
run on a separately bounded coordinator CPU/I/O executor. Admitted stream owners
survive caller cancellation until accepted commands and cleanup finish, keeping
their capacity and scratch ownership throughout. Global finalization still
precedes every table-object upload.

Accepted [A09](../server/notes/DN-A09.md) pins execution ownership: disk-backed
state stays in bounded registered disposal cells, commands borrow it only on
CPU/I/O workers, and async replies carry memory-only data or opaque leases.
Process and operation anchors are created inside worker commands after empty
cells are registered. Admission remains charged through cleanup and actual
supervisor reaping; cancelled drain waiters preserve registered handles.
Premature pool shutdown is an explicit failed lifecycle with bounded retained
owners, never successful cleanup or idle. Normal shutdown drains owners and
anchors before stopping WorkerThreads. Owner, receive and send remain separate
implementation minis; design adoption is not execution evidence.

Keep local dispatcher compatibility for existing standalone tests. In cluster mode use real HTTP for peer/self dispatch and never silently execute on the API host. Bounded retries of pure jobs preserve logical identity and discard duplicate/late results. Publication retries use durable decision lookup, not job retry rules.

## Causal acceptance evidence

Use actual separate local-store, API and worker processes over TCP. Start with two API replicas behind one small test front door and two workers; add at most one temporary worker. Each worker has private scratch and separate process identity. Test real partition execution, operation symmetry, >1 concurrent query per node, saturated-operation peer progress, whole-batch atomicity, competing writes, lost response/replay after a later commit, startup lag, safe drainage, autoscaling, controller/API/worker replacement and persisted policy.

Deterministic gates/events at real admission, partition execution and publication boundaries are available only behind an explicit `acceptance` build feature and protected test configuration. Normal builds do not expose them. Bound event queues and gate lifetimes; record operation/job/node/incarnation/PID identities and occupancy without user data/secrets. Assertions use observed simultaneous occupancy and ordered gates, not timing guesses or client-start barriers alone. Reuse existing CRUD/schema tests and runner patterns; do not build a second generic testing framework.

The local store process proves conditional-write and restart behavior only within its stated failure domain. Native subprocesses do not prove separate-kernel/cloud-VM behavior. Retain sanitized reports and state the actual topology. Docker remains optional. Final gates include both independent reviews, targeted then integrated Rust/Python checks and complete actual-process acceptance; no pending/stubbed functionality earns a completed checkbox.
