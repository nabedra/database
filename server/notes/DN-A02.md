# DN-A02: shared publication, replay, and controller fencing

Design only. The current shared checkout is the source of truth. This note proposes the persistence contract; it changes no engine, SDK, or deployment behavior.

## Recommendation

Keep immutable Parquet objects and one conditional catalog replacement as the database publication point. Add an immutable linked decision history whose head is published in that same catalog replacement. Use a separate conditional control document for desired capacity, controller ownership, membership, and lifecycle commands. Heartbeats have separate per-incarnation records. All of this needs strongly consistent reads, immutable create, and atomic compare-and-swap on individual keys. It needs neither multi-key transactions in the core nor a new consensus protocol.

The first local acceptance backend should be a small TCP HTTP object service backed by SQLite on its own local disk. API and worker processes use this service, not an in-memory fake or a shared directory pretending to implement conditional writes. Cloud adapters must satisfy the same contract. AWS remains an optional adapter, not a core or Python SDK dependency.

The tradeoff is a globally serialized database publication point and initially linear request-history lookup. These preserve the current one-table atomic mutation model with a small change. Partition computation still runs concurrently across identical workers. A later durable request index can accelerate lookup without changing the publication rule.

## Evidence in the checkout

| Location | Existing behavior and consequence |
| --- | --- |
| `server/src/commit.rs:28`, `:44` | `Coordinator` owns a Tokio mutex and loads a fresh catalog after acquiring it. Separate engines in separate processes have separate locks. |
| `server/src/commit.rs:149`, `:245`, `:265` | Publication validates one changed table, bounds catalog encoding before uploads, creates immutable objects, then replaces `catalog.json` using its loaded ETag. Preserve these validations. |
| `server/src/commit.rs:277` | Ambiguous publication does one read and recognizes only the current commit UUID. A later successful commit hides this evidence. |
| `server/src/storage.rs:129`, `:142`, `:239`, `:330` | `ObjectStore` already has Create/Match conditions and forbids automatic PUT retries. S3 maps them to conditional requests; catalog initialization handles a competing creator. The trait comment does not yet promise all the consistency/durability properties below. |
| `server/src/storage.rs:133`, `:239`, `:316` | The generic download exposes AWS `ByteStream`; replacement and key guards currently allow only the catalog. New typed metadata keys and a provider-neutral byte stream are required. |
| `server/src/storage/manifest.rs:19`, `:113`, `:142`, `:307` | The catalog contains only latest revision/UUID/tables, has a 1 MiB bound, and strictly decodes supported wire shapes. Replay history needs an explicit new format version. |
| `server/src/engine.rs:89`, `:129`, `:201` | Execution receives only `Operation`, not the request ID. Create assigns a fresh table UUID; drop addresses an immutable table ID. Retrying create by reexecution would lose the original descriptor. |
| `server/src/engine/write.rs:73`, `:188`, `:199`, `:259` | Empty batches and unchanged mutations return without publishing. Changed batches publish once. Replay must cover both paths while keeping result shapes. |
| `server/src/api.rs:141`, `:151`; `server/src/api/response.rs:15` | Request IDs are correlation metadata today. Execution discards the ID and response encoding happens after commit, with Unknown on mutation encoding failure. |
| `server/tests/storage.rs:20`; `server/tests/commit.rs:184`, `:208` | Current tests cover conditional initialization, ambiguous latest-UUID confirmation, and a deterministic gate holding one local mutex. They do not establish cross-process write safety or durable replay. |
| `server/tests/support/storage.rs:30`; `server/src/main.rs:83`; `server/Cargo.toml:20` | The fake serializes a map under a process mutex; startup selects S3 and AWS dependencies are unconditional. Neither establishes a cloud-neutral runnable path. |

## Required backend contract

Namespace identity must name one durable database/control domain. Every API and worker in a cluster uses the same configured domain. Credentials and tokens stay out of stored request identities and diagnostic output.

1. A GET returns one complete version with its opaque, nonempty version token, or definite NotFound. Successful reads are linearizable, including after another process's completed write. Immutable objects become readable before any root referencing them can become visible.
2. Create succeeds only if the key is absent at the write's atomic decision point. Match succeeds only if the supplied token names the current version at that point. The condition and byte replacement are atomic across all clients. At most one of two writes matching the same version succeeds.
3. Every successful mutable write gets a new version token. Tokens must not exhibit ABA, even when bytes are identical. Roots cannot be deleted/recreated while clients run. Revision counters in JSON are validation data, not a replacement for backend conditional writes.
4. A successful response means durable bytes and metadata within the adapter's declared failure domain. A restarted service must recover every acknowledged write. State the actual failure domain; local-disk durability does not promise survival of machine/disk loss.
5. Conflict means this particular conditional attempt definitely did not apply. Unavailable means definitely not submitted/applied, or a read failed. Any possibly applied PUT, timeout, broken response, or missing response token is Ambiguous. Never label a possibly applied write NotApplied. Adapters disable hidden PUT retries; reconcilers own subsequent attempts.
6. Immutable objects reject replacement. An ambiguous immutable upload can be confirmed by exact byte comparison at its unique attempt key. A mismatching existing value is corruption or a key collision, not a reusable object.

A read followed by unconditional write, atomic filesystem rename alone, object listing, eventual consistency, and a process lock fail this contract. Advertise no distributed support for an adapter until its conformance tests pass. Reads of roots and referenced objects need no application lock; readers pin one catalog and its immutable references throughout an operation.

## Database publication and durable replay

Introduce catalog format v3 with one additional `decision_head` reference. Each immutable decision record contains a unique attempt/commit UUID, parent decision reference, base catalog revision, new catalog revision, normalized request UUID, request identity, and the terminal response status/body. Use typed namespace-relative keys such as `decisions/<attempt-uuid>.json`. The root continues to carry the current validated table manifests. A decision record contains no duplicated full catalog.

For the smallest first implementation, request identity is exact equality of the original validated UTF-8 request body, scoped to the durable database namespace and protocol version. Store those bounded bytes in the decision. Do not hash a generic JSON Value that may change numeric types. This deliberately treats reordered fields or whitespace changes under the same UUID as a mismatch. The SDK must retain and resend the original body. A later version can define typed canonicalization if semantic equivalence is needed. Authentication still runs before lookup; changing a bearer token does not change the body identity.

Only mutating operations consume IDs in this durable ledger. Ordinary reads retain their current fresh-read semantics and request IDs remain correlation IDs. For mutations, reuse of an ID with a different identity returns Conflict/NotApplied for the new payload and leaves the prior decision intact. The original matching payload can still retrieve its result. Document this scope; do not silently promise snapshot replay for reads.

Publication proceeds as follows:

1. Load a fresh catalog/version. Search its reachable decision history for the UUID. Return its stored response if the body matches; reject a mismatch. If any required history object is unreadable, corrupt, or traversal exceeds the per-attempt work budget, return an availability/Unknown result as appropriate. Never interpret incomplete lookup as absence.
2. If absent, validate and prepare the operation against this catalog, using the current engine's schema, table-ID, key, batch atomicity, and partition accounting rules. Peer jobs prepare private outputs only. The coordinator owns final validation and publication. Retain local admission and the local mutex if useful for bounded work, but correctness does not depend on either.
3. Produce the exact bounded terminal response before publication. Create newly required immutable partition objects, then a decision record containing that response and the prior decision head. No-op success and deterministic operation errors also produce decision records, with unchanged table manifests. Pre-authentication/framing errors and transient overload/storage/Unknown responses do not consume an ID.
4. Match-replace the catalog with both the new table state and new decision head. Advance its revision and commit UUID even for a decision-only publication. Keep the 1 MiB catalog limit; bound each decision separately to at most two `MAX_BODY_BYTES` plus a small fixed metadata allowance, with explicit encoder enforcement. Refuse oversized candidates before any upload.
5. Return the stored response only after publication is confirmed. No acknowledged success can exist without a reachable decision. The actual CAS is the single visibility point for both data and result.

Change the one-table validator to allow either exactly one changed table or zero table changes plus one new decision. Do not allow multiple changed tables. Existing mutation response shapes remain null, count, found boolean, or create descriptor. The original create descriptor, including its table UUID, survives drop/recreate and all later commits. A no-op delete must replay its original false even if another operation subsequently inserts the key. A recorded duplicate-key error must remain that error even if the table later changes.

On a definite CAS conflict, discard that candidate as an uncommitted attempt, reload history first, and recompute against the new catalog if no decision exists. Never reuse a candidate built from stale manifests. A bounded internal retry budget can reduce newly exposed contention errors; exhausting it is a retryable conflict. It cannot be reported as definite NotApplied for the logical request if another attempt with that ID has an unresolved publication. In that case return Unknown unless a durable terminal decision has been confirmed.

Two candidates built from the same root cannot both win. If they have the same request ID, the loser finds the winner's decision after reload. If they have different IDs, the loser recomputes, so a later successful publication retains the earlier decision as an ancestor. This gives a serial order across processes without distributing the current mutation mutex.

### Ambiguous publication and failover

After a lost CAS response, read the latest catalog and traverse its history. Finding the matching decision anywhere in that history proves publication, including after superseding commits. An uploaded record that is not reachable proves nothing. Another worker can recover using the same request body and UUID without the original process's memory.

Absence in a fresh read alone does not prove failure: the original conditional request may still be pending. Return Unknown if recovery cannot finish. A retry with the same ID may prepare and publish a new candidate only after a complete lookup against its newly loaded base. Its successful CAS is safe: if the old pending attempt wins first, the retry's CAS fails; if the retry wins first, the old attempt's base token is fenced out. If the new base already supersedes the old base, that old attempt is already fenced out. After each ambiguity or conflict, repeat lookup before any new attempt. Never blindly resend the old PUT or change UUID to escape Unknown.

For a definite terminal error/no-op after an earlier unknown attempt, publish its decision-only CAS before returning the terminal result. That CAS both records the result and closes the window in which an earlier pending write could apply. If that CAS is itself ambiguous, the result remains Unknown until confirmed.

Immutable upload failure before any catalog attempt leaves the database unchanged. Crash after uploads leaves harmless orphans; crash after CAS leaves a recoverable decision. Do not delete objects to undo uncertainty. Initial release retains all decision records and referenced data; add no garbage collector or retention TTL. History scanning has growing cost and storage use. A bounded scan must fail safely, so a durable index/checkpoint retaining every request identity/outcome is the next scaling improvement, not a prerequisite for correctness. Claim indefinite replay only while the history remains retained and accessible. Restoring an old backup creates a new namespace identity; silently rolling back the same namespace would violate acknowledged outcomes.

Upgrade a v1/v2 catalog with an exclusive maintenance migration into v3 and an empty decision head. Preserve existing table UUIDs, immutable objects, and manifests. Reject v3 in old binaries; mixed old/new writers are unsupported. Operations before migration have no replay guarantee. The current initialization path must never reset malformed or unsupported catalogs.

## Control metadata and worker lifecycle

Use a separate bounded `cluster/control.json` CAS root so heartbeats/scaling do not contend with database publication. Store cluster identity, config generation, desired count or autoscale bounds/policy, cooldown state, controller owner/epoch/deadline, node slot/incarnation records, and pending lifecycle commands. Persist admin scale/autoscale changes with request IDs and durable outcomes too; they can use a separate immutable control decision chain attached to this root. Its updates must preserve current owner/epoch and pending commands unless explicitly transitioning them.

Each node record needs a stable logical slot ID, unique launch incarnation, provider resource ID when known, address, declared capacity, and state. Use provisioning, starting, ready, draining, stopped, and failed states with explicit transitions. Persist command intent before invoking `start_node` or `stop_node`. Pending commands have stable IDs, target incarnation, generation, last known result, and retry state. Never reuse a stopped incarnation for a replacement machine. `list_nodes` must expose these identities so takeover can discover a created resource whose start response was lost.

Heartbeats go to `cluster/heartbeats/<incarnation>.json`, with monotonically increasing sequence, readiness, bounded capacity/load, and active job count. Only that incarnation writes its heartbeat using Create then Match. The controller reads keys named by control state, not a possibly incomplete LIST. A restarting process gets a new incarnation. Readiness requires successful storage/catalog validation, a listening authenticated worker endpoint, initialized execution capacity, and completed registration. A heartbeat reporting ready does not override a durable draining/failed state.

For missed heartbeats, use the controller's monotonic elapsed time since observing a sequence advance. On controller takeover, allow a full observation interval before declaring an existing worker dead. Worker wall clocks cannot extend their own membership. Routing uses fresh-enough heartbeats plus durable readiness; workers enforce their own bounded admission and reject stale incarnation or drain requests. Multiple queries may share a worker only within those limits. The ORM still targets the API endpoint; storage membership never becomes an ORM routing API.

Drain first persists a durable draining state, stops new routing/admission, and waits for admitted jobs and reconciliation. Completion acknowledgement names the incarnation and drain command. Forced stop after a configured deadline may leave operations Unknown; their IDs recover through the database decision history. A missing heartbeat is suspicion, not proof of stopped computation. A stale coordinator might still win a database CAS, so never claim its operation failed solely because its machine was removed from routing. Catalog CAS and request identity continue to protect atomicity.

## One controller with fencing

Controller acquisition or takeover Match-replaces the control root, increments a never-reused u64 epoch, sets a fresh process owner ID, and records a deadline. Renewal preserves the epoch and owner. Fail closed on epoch exhaustion. Every controller transition, renewal, command claim, and completion checks the loaded owner/epoch and uses that exact control version token. On any conflict it reloads and stops acting if ownership changed. The controller lease grants no exclusive database-writing role; every ready worker can coordinate operations.

With only generic CAS, the deadline determines eligibility to attempt takeover; the successful takeover CAS is the fencing point. Expiration is not an independent backend-enforced revocation. Controllers use bounded clock skew for deadline comparisons, renew well before expiry, and stop issuing work if they cannot renew. Clock jumps can cause premature takeover or delayed recovery, but cannot authorize stale control CAS after takeover. If strict expiry-time enforcement is required, use an adapter with authoritative lease time and conditional expiry checks; do not claim that a JSON timestamp supplies it.

Storage fencing cannot revoke an external callback already in flight. Before each callback, persist a command claim under the current epoch. Invoke/retry using the same durable command ID and target incarnation. Reconcile through `list_nodes` before creating replacements or assuming a lost stop succeeded. The adapter must provide idempotent creation by command ID, idempotent exact-incarnation stopping, and inventory sufficient to discover late results. Ideally it also rejects older epochs. A resumed old callback may complete, but its stale completion cannot update control state, duplicate starts collapse to one resource, and an old stop can only address the old incarnation. A new controller adopts unresolved commands rather than inventing new IDs for the same side effect.

Arbitrary callbacks that launch an unlabelled machine on every call cannot guarantee durable recovery. This is a real callback contract requirement. The public `serve(name, nodes, start_node, stop_node, list_nodes)` can remain small, but its callback input/output must carry stable command/incarnation identity through a context/descriptor. Parent must settle that exact callable shape before implementation. No mandatory EC2-specific fields belong in it.

The controller persists desired state, pending provisioning/drain work, and cooldown timestamps before acting. On takeover it reconciles those intents and inventory before applying a new autoscale decision. CPU/queue samples can be transient; bounds, desired target, cooldown, and incomplete actions must survive restart. A fixed maximum node count and encoded control-document limit bound this first version; refuse a scale request exceeding either rather than silently dropping state.

Continue inventory reconciliation after takeover, since a previously timed-out start can appear late. Adopt it if its durable intent still needs capacity; otherwise drain/stop that exact resource through a new persisted command. Limit reconciliation to resources labelled with this cluster and command identity. Keep unresolved commands discoverable until their effects are resolved; a timeout alone must not erase an intent. Initial control creation uses Create, and later takeovers always preserve/increment its epoch rather than resetting the document.

## Local backend and cloud-neutral integration

Implement the local acceptance service with Python standard-library `sqlite3` plus a bounded threaded HTTP handler. One SQLite file stores namespace-qualified key, BLOB bytes, and a monotonically increasing durable generation. A PUT uses `BEGIN IMMEDIATE`, checks Create/Match, stores bytes and the next generation, commits, then sends the token. GET reads bytes/token in one fresh transaction. Return busy-before-write as definite unavailable; failures during/after commit are ambiguous. Do not use a process mutex as the atomic-write implementation.

Keep the file on the service host's local disk. For WAL mode, use `synchronous=FULL`, retain WAL recovery files, and verify the effective settings. SQLite documents [one simultaneous writer and immediate transactions](https://www.sqlite.org/lang_transaction.html), [WAL's same-host requirement](https://www.sqlite.org/wal.html), and [synchronous durability settings](https://www.sqlite.org/pragma.html#pragma_synchronous). This service is a local acceptance dependency, not a new HA database product. It can restart from disk but is a single availability/failure domain. Run API, at least two workers, and the persistence service as actual separate processes connected over TCP.

Production adapters can implement the same conditional-object contract using an existing strongly consistent object service or a transactional database that atomically checks version and updates one key. Separate immutable blob and metadata implementations are allowed only if durable upload/read visibility precedes root publication. Endpoint compatibility alone does not prove conditional semantics. Adapter implementations, not the core, own provider clients, credentials, transport retry configuration, and conformance evidence.

Replace AWS `ByteStream` in `Download` with a bounded provider-neutral chunk reader/stream. Move S3 implementation and connection setup behind an optional feature/module; feature-gate AWS imports and dependencies. Provide explicit local HTTP store configuration and worker/API startup selection. Keep key validation, read bounds, sanitized errors, and catalog initialization in shared code. New mutable metadata keys must be explicitly typed/allowlisted; never weaken all snapshot keys into general overwriteable strings. The Python ORM and distributed-node SDK need no cloud library.

## Small implementation slices and ownership

These are proposed assignments, not implemented changes. Keep overlapping file owners sequential.

| Slice | File owner | Reviewable result |
| --- | --- | --- |
| Backend contract and provider separation | Storage owner: `server/src/storage.rs`, new `server/src/storage/{backend,s3,http}.rs`, `server/Cargo.toml` | Neutral download type, explicit guarantees/errors, typed metadata keys, optional S3; existing bounds and immutable guards retained. |
| Local persistence service | Acceptance owner: new `scripts/local_store.py` and focused process fixture | SQLite GET/Create/Match over TCP, durable generations, fault gates around commit/response; no engine logic. |
| Versioned history | Manifest owner: `server/src/storage/manifest.rs`, new `server/src/storage/decision.rs`, `server/tests/storage.rs` | v3 read/write and explicit migration; bounded records and safe complete lookup. Coordinate with storage owner before editing exports. |
| Atomic result publication | Commit/engine owner: `server/src/commit.rs`, `server/src/engine.rs`, `server/src/engine/write.rs`, `server/src/api.rs`, `server/src/api/response.rs`, focused existing commit/API tests | Carry request context to mutation, prepare result before CAS, receipt-only decisions, replay after superseding commits. Keep the old direct execute path clearly scoped to internal/non-replay use or route it through a generated context. |
| Durable control state | Control owner: new `server/src/cluster/{state,controller}.rs` plus narrow startup wiring | Fenced root CAS, durable commands, heartbeat records, reconcile/takeover; provider callbacks remain injected. Wire `main.rs` only after backend selection is ready. |

## Minimal meaningful verification

No tests were run for this design task. Preserve current behavioral tests; add these focused checks instead of mirroring every helper.

1. **Backend atomicity across TCP processes.** Gate two independent clients after reading the same token, release both PUTs, and prove exactly one success. Restart persistence after an acknowledged write and confirm bytes/token. Repeat with immutable Create and verify overwrite rejection. Use explicit barrier messages, never timing sleeps.
2. **Lost response followed by superseding commit.** Apply request A, drop its CAS response, then commit B and kill A's coordinator. Another worker retries A and returns the original response with no extra table change. Cover create's UUID and a no-op delete whose row later appears as a small parameterized case.
3. **Pending original publication.** Gate A before its CAS decision, run same-ID recovery, and release attempts in each order. Prove one reachable decision and one mutation. Add a different-body same-ID case; only one identity wins and the other receives conflict. A failed/incomplete history read must never trigger reexecution.
4. **Atomic preparation failure.** Fail one partition upload in a multi-partition mutation. The old catalog and every old row remain visible. Kill after receipt upload but before CAS; replay must ignore that orphan. Existing partition atomicity fixtures can supply this case.
5. **Controller takeover and delayed callbacks.** Gate an old controller's command and renew/control CAS, take over with a new epoch, then resume the old controller. Its metadata updates fail; the fake provider deduplicates the stable start command, and an old stop cannot kill the replacement incarnation. Repeat with provider action applied but response lost and reconcile via inventory.
6. **Drain/recovery with actual workers.** Persist drain during an admitted mutation, kill its coordinator after publication, and restart the API/controller. No new jobs enter the draining incarnation; the same request ID recovers the stored outcome and pending lifecycle intent completes. Restarting a worker under a new incarnation must not revive the old heartbeat.

Have the parent run a spec review of the guarantees and callback shape before assigning code. After those slices, run a separate quality review focused on ambiguous writes, stale epochs, namespace validation, and accidental AWS coupling. Neither review needs a broad rewrite or a large new test matrix.

## Decisions the parent must carry forward

The recommended correctness contract is settled above. Three product tradeoffs need explicit acceptance: exact-body mutation identity versus typed canonicalization; indefinitely retained linear history versus an indexed history in the first release; and takeover-CAS fencing versus strict backend-enforced lease expiration. The callback context/descriptor shape is the one interface decision that blocks safe lifecycle implementation. Without stable idempotency and incarnation identity, promise only best-effort provisioning recovery, not durable exactly-once resource creation.
