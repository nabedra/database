# DN-A03 public administration, bootstrap and process acceptance design

Design complete, 2026-11-10. Only this note was written. No production implementation, suites, acceptance runs, builds, installs, Docker startup, cloud calls, dotenv reads, commits, branches, worktrees or delegation occurred. The shapes below are proposed locked contracts for P01 reconciliation with A01/A02, not implemented behavior. Remaining integration decisions are listed at the end.

## Authoritative baseline

- `orm/src/meshdb/__init__.py:1` exports DB, query/table types and existing errors. Add only `serve` and `Cluster` as public runtime objects; callback/status records are ordinary dictionaries, with internal typing aliases if useful.
- `orm/src/meshdb/client.py:76` validates connection configuration without I/O. `:124` posts strictly bounded envelopes, rejects redirects, never retries, and distinguishes lost mutation responses from definite rejection. `:230` constructs DB with one endpoint. Keep its constructor, operation payloads and data-result semantics.
- `orm/src/meshdb/errors.py:9` already carries code, request_id, status_code and outcome. Reuse these errors, including UnknownOutcomeError, without introducing a separate exception hierarchy.
- `orm/pyproject.toml:5` requires Python 3.11 and no runtime dependencies. There is no installed `meshdb` console entry point yet.
- `server/src/cli.rs:14` has only `meshdb-server serve`. `server/src/config.rs:14` configures local worker threads, queue size, partition size and TLS; `:108` currently requires S3 settings and the data token. `server/src/main.rs:78` connects S3, then creates the local executor/pool. Cluster mode needs a separate API bootstrap, with no API-local partition fallback.
- `server/src/commit.rs:27` owns a process-local mutation mutex. It cannot become a cluster write lock merely by launching more processes. A02's shared atomic publication and durable operation records are dependencies of any distributed write claim.
- `scripts/server_process.py:22` owns one child, private logs, readiness polling, restart identity and shutdown/reaping. `:30` calls S3-specific `config.server_env`; reuse its lifecycle patterns, not that configuration dependency. Its temporary port reservation closes before Popen, so it does not reserve the port through child binding.
- `scripts/acceptance_support.py:88` and `:157` are explicitly AWS/dotenv helpers. Do not call or import their configuration path in the new local runner. `scripts/acceptance_concurrency.py:110` also shells out to AWS for storage inspection.
- Reuse exact-value comparisons and check reporting from `scripts/acceptance_http.py:23` and `:36`, TLS material from `scripts/test_tls_material.py:52`, cancellation expectations from `scripts/test_acceptance_cancellation.py:16`, and fixture semantics from `scripts/acceptance_partitions.py:22`, `:29`, `:41`. Existing client-start Barrier use at `scripts/acceptance_concurrency.py:214` proves simultaneous submission only, not remote execution overlap.

The completion/testing contracts are `plans/distributed-nodes.md:7`, `:17`, `:104`, `plans/partition-workers.md` validation policy, and `plan-v0/implementation.md`. Historical notes are evidence of prior behavior, not proof of the new deployment.

## Exact public Python interface

```python
serve(name: str, nodes: int | tuple[int, int] = 1,
      start_node=None, stop_node=None, list_nodes=None) -> None

Cluster(api_url: str, admin_token: str, *, timeout: float = 30.0,
        ca_file: str | None = None)
Cluster.scale(nodes: int, *, wait: bool = False,
              timeout: float = 300.0) -> dict
Cluster.autoscale(*, min_nodes: int, max_nodes: int) -> dict
Cluster.status() -> dict
```

Construction and local argument validation send no requests. `serve` is blocking, starts the separate API runtime and callback bridge, and returns after orderly shutdown. It does not deploy API hosts, start application queries or return a background cluster object. Importing meshdb starts nothing. A clean SIGTERM/SIGINT closes admission and drains the API; losing an API supervisor does not terminate the worker fleet or erase policy. Runtime startup failures raise a sanitized MeshDBError; local input/configuration failures raise ValidationError. The console wrapper maps failures to exit 1 and SIGINT to exit 130.

Validate name with the existing identifier contract `[A-Za-z_][A-Za-z0-9_]{0,63}`. Counts must be exact integers, excluding bool, in 0..1024. A tuple must have exactly two counts with `0 <= min <= max <= 1024`; `(0, 0)` is allowed. Reject lists, floats, reversed ranges and partial callback sets. All three callbacks must be synchronous callables or all must be None. Reject declared coroutine functions and awaitable results. `wait` must be bool and seconds must be finite positive int/float, excluding bool. Deployment's maximum fleet limit may be lower, and the server validates it independently.

An integer creates a fixed desired count. A pair creates an autoscale policy whose initial desired count is its minimum. `scale` persists fixed mode and disables autoscaling. `autoscale` persists the new bounds and clamps the current desired count into them. Zero is a supported explicit drain-to-zero target. Autoscale with a zero minimum must observe API-side queued demand so it can wake from zero; worker-only utilization is insufficient.

`scale` returns the committed policy acceptance record, not ready-machine capacity. The exact record is `{revision, mode, desired, min_nodes, max_nodes}`. Mode is `fixed` or `autoscale`; fixed mode sets both bounds equal to desired. Revision is a positive monotonic integer. Identical policy submissions return the existing revision and never create duplicate lifecycle intents. Distinct simultaneous policies use durable serialization; the last committed revision wins.

With `wait=True`, poll status until the accepted revision is still current, ready count equals desired, and no launch/drain/termination intent remains outstanding. Completed retained intents do not block convergence. Return the same acceptance record. If another revision supersedes it, raise ConflictError with code `policy_superseded`; never wait for an obsolete target. The wait budget starts before the submission, uses monotonic time, and bounds each subsequent I/O attempt by the smaller of remaining budget and constructor timeout. A wait expiry after confirmed acceptance raises TransportError with code `wait_timeout`, outcome None, and the submission request_id. The persisted policy remains active. Document explicitly that this error does not roll back the accepted policy. Do not label accepted policy changes themselves not_applied in a server response.

Constructor timeout retains the existing blocking socket-I/O meaning for ordinary calls. Waiting provides an additional overall deadline, with connections and response reads using the remaining budget; an arbitrary sequence of small reads must not extend that deadline. A timeout before a valid policy acknowledgement is UnknownOutcomeError. No public method automatically resubmits an ambiguous mutation. The exception's request_id and `status()` allow investigation. Reissuing the same desired policy is safe, but can supersede an intervening different policy, so it is a deliberate new command.

## Administration wire contract and authorization

Use a separate `POST /v1/cluster/operations` endpoint under the same configured API base path. Requests retain `{request_id, operation, payload}` and responses retain the existing `{request_id, data}` or `{request_id, error:{code,message,outcome}}` envelopes. Operations are `cluster.scale` with `{nodes}`, `cluster.autoscale` with `{min_nodes,max_nodes}`, and `cluster.status` with `{}`. Every policy command has a UUID request_id, canonical payload digest and durable outcome. Replay of the same identity/payload returns the same acceptance record; a different payload under that identity returns conflict. A02 defines outcome retention. Expired identities must be rejected rather than silently executed again.

The data endpoint remains `/v1/operations`. Its request_id continues to be diagnostic to existing ORM callers; a fresh public DB call receives a fresh identity. Internal gateway/coordinator retries preserve the original identity and use A02's durable result. Do not add transparent Python data retries or imply an application can safely repeat a new insert after UnknownOutcomeError. Raw-wire acceptance may replay the identical envelope to exercise supported durable retries across API loss.

Use independent data, admin and worker-enrollment credentials. Admin credentials authorize only cluster routes; data credentials authorize only data routes. Worker session credentials authorize only internal membership/job routes and are scoped to cluster/node incarnation. Neither client token grants node enrollment or callback access. Unauthenticated requests return the same sanitized unauthorized response; validate authorization before accepting work. Public liveness/readiness exposes only a boolean result. Status, provider identities and capacity are admin-only. Verified TLS/custom CA and redirect rejection follow the existing transport; plaintext HTTP is limited to explicitly configured local/private deployments. Never forward public bearer tokens to provider callbacks.

Control request/response bodies have a 1 MiB maximum, at most 4096 node/intent records, and reject duplicate JSON fields, nonfinite numbers, unsupported fields and wrong exact types. A configured fleet ceiling of 1024 leaves bounded room for replacement/draining records. History is compacted separately, never silently truncated in status. Server errors use existing validation/400, unauthorized/401, conflict/409, limit_exceeded/413, overloaded/429, storage_unavailable/503 and outcome_unknown/503 mappings. An absent serving node produces storage_unavailable/503 with not_applied before dispatch. `unsupported_capability`/400 maps to MeshDBError for externally managed provisioning. Unknown valid server codes remain MeshDBError, preserving the current fallback.

Keep a distinct private admin transport using shared URL/TLS/JSON validation where useful. Do not expand the data operation allowlist or change legacy response handling merely to reuse `_Transport`.

## Exact callback records and ownership

Callbacks run in each API supervisor's Python process, next to its Rust API child, never in DB/Cluster application processes or on workers. Rust owns durable reconciliation, leases, policy and lifecycle intents. The lease holder invokes its supervisor's callbacks through a loopback-only authenticated JSON bridge. It persists an intent before invocation. Standbys load the same adapter binding but issue no lifecycle commands until they acquire authority. The bridge verifies the current lease/intent before admitting a command. A stale callback already executing may finish, so fencing alone cannot replace provider idempotency.

`start_node(request) -> str` takes this exact JSON-compatible record:

```text
{
  "version": 1,
  "cluster_id": "UUID",
  "cluster_name": "identifier",
  "request_id": "UUID",
  "node_id": "UUID",
  "bootstrap": { ... worker file record below ... }
}
```

The request_id is the durable provisioning intent identity, not a transient HTTP attempt. node_id is allocated before launch and remains stable for that intent. A later replacement uses fresh identities. Return a nonempty opaque resource_id string of at most 256 UTF-8 bytes without control characters. Never treat resource_id as a shell command, PID by itself or network endpoint. Cluster/node/request identities must be saved as provider labels or equivalent durable adapter records before returning. Repeated starts with the same request_id must converge on the same resource, including across supervisor death. The callback must not wait for MeshDB readiness. It returns after provisioning is durably accepted and bootstrap can proceed.

`stop_node(resource_id: str) -> None` is idempotent. It requests removal of exactly the owned resource and accepts already-absent resources. Returning does not prove termination. `list_nodes() -> list[dict]` returns a complete bounded inventory of resources owned by this configured cluster:

```text
{"resource_id": "opaque", "cluster_id": "UUID", "node_id": "UUID",
 "request_id": "UUID", "state": "starting|running|stopping|terminated"}
```

Require unique resource_id and unique active launch identities. A duplicate resource for one intent is quarantined and reported as conflict, never counted twice or adopted by endpoint alone. `list_nodes` is cluster-bound by the user's callback closure/configuration; it has no required cluster argument. It must report launches whose start response was lost. Never serialize Python functions, closures or provider clients into metadata.

Inventory can lag. Positive matching records recover identities; absence alone never proves an ambiguous launch failed. A retry uses the same request_id and an adapter that guarantees idempotent creation. A definitive terminated record, or the adapter's documented authoritative absence guarantee, completes termination. Adapters with neither idempotent creation nor authoritative identity recovery are unsupported for managed mode. Never create a new launch identity merely because a callback timed out or inventory omitted a pending resource. Persist recovered resource IDs before further action. Unknown owned resources are quarantined for reconciliation rather than terminated automatically.

Callbacks must return within their own infrastructure I/O deadlines. The bridge has a fixed pool of four callback slots and no unbounded submission queue, with one reserved for inventory. Default observation deadline is 30 seconds. An expired start/stop is ambiguous, not a failed side effect; quarantine its slot until completion, reconcile inventory and retain the intent. Do not spawn replacement threads for hung callbacks. A saturated bridge reports degraded control and stops new provisioning. Python cannot forcibly cancel an arbitrary running callback safely; this contract does not promise hard cancellation. Repeated adapter errors use persisted capped backoff; restarts do not reset the attempt budget. Separate action and inventory slots allow ordinary slow starts to be reconciled. A permanently hung inventory callback still requires operator intervention or supervisor restart.

The small local process adapter must persist its own launch registry outside the API process, with serialized launch-by-request_id, before reporting success. A shared in-memory Popen dictionary fails takeover requirements. Its resource identity includes a random nonce and verified process start identity, not just a recyclable PID. It launches worker processes independently of the API process group so killing the API does not kill workers. A callback pool deadline is never permission to kill a worker with accepted work.

## API configuration, restart and worker bootstrap

Keep deployment settings outside the five serve arguments. `MESHDB_CONFIG` names a versioned JSON file, and `MESHDB_SERVER_BINARY` optionally selects the existing Rust binary. Neither is loaded during import or Cluster construction. The API file has exactly these top-level keys:

```text
version: 1
cluster_id: UUID
bind: "host:port"
advertise_url: absolute HTTP(S) base URL for this API replica
api_urls: list of 1..16 routable API base URLs or one stable service URL
control: {driver: string, options: object}
objects: {driver: string, options: object}
auth_file: local protected path
tls: null | {cert_file: path, key_file: path, ca_file: path}
worker: {listen: "host:port", operation_slots: int,
         partition_slots: int, queue_size: int, partition_size: string}
```

The auth file contains distinct `admin_token`, `data_token`, `enrollment_token` values and is never included in status. Backend options are validated by the selected adapter, with unsupported consistency capabilities rejected at startup. First supported local deployment uses filesystem objects and a durable control backend shared across API processes, selected by A02. Existing S3 remains an optional adapter. Do not make generic config require bucket, region, EC2 or AWS credentials. API listeners may have different local addresses but must name the same durable cluster and immutable namespace/configuration fingerprint.

Initial serve atomically creates cluster configuration by cluster_id, binds name uniquely and persists the initial policy, backend/namespace fingerprints, credential fingerprints, worker template, callback mode and adapter version. Restarts check identity/compatibility and attach to persisted desired state; serve's nodes value is initialization-only. An incompatible name/backend/template/auth fingerprint fails startup explicitly rather than overwriting the cluster. Policy changes occur through Cluster calls. Adapter callables and secret files must be supplied again on each supervisor restart; they are deployment code/configuration, not stored executable state. A replica missing the required managed adapter cannot become controller. The API may remain available with degraded controller status. Policy and pending intents survive complete API downtime.

Public CLI is `meshdb worker --bootstrap PATH`. Add a tiny Python console wrapper forwarding an argv list to `meshdb-server worker --bootstrap PATH`; alternatively operators can call that Rust command directly. No shell evaluation, package download, Docker requirement or Python worker execution. Existing `meshdb-server serve` remains the legacy local mode. The internal API child command is `meshdb-server api --config PATH`; its bridge address/credential are passed privately by the supervisor, never in public command arguments or durable status.

The worker bootstrap file has exactly these keys:

```text
version: 1
cluster_id: UUID
node_id: UUID
request_id: UUID | null
api_urls: nonempty bounded list of API base URLs
listen: "host:port"
advertise_url: absolute worker HTTP(S) base URL
control: {driver: string, options: object}
objects: {driver: string, options: object}
enrollment_token: secret string
tls: null | {cert_file: path, key_file: path, ca_file: path}
operation_slots: positive int
partition_slots: positive int
queue_size: nonnegative int
partition_size: unit-bearing string
```

`request.bootstrap` is this record with advertise_url initially null. The callback materializes it on the target machine, fills only the advertised address and target-local TLS/adapter credential paths, and writes a protected file. The callback must preserve cluster/node/request identities, backend namespaces and capacity settings. Worker startup rejects a null advertised address. api_urls include failover replicas or a stable service endpoint and are copied from the persisted API configuration. Credentials are delivered through protected files or an operator's secret delivery mechanism, never logs/status. File locations are local to the target, not paths the API assumes it can mount remotely.

Worker startup validates configuration, binds the job listener, checks shared storage/control capabilities and reads the committed catalog before registration. It registers node_id, a fresh boot/incarnation UUID, endpoint, capacity and launch identity, then receives a scoped internal session credential. Managed registrations must match an existing launch intent. Registration may precede resource_id recovery; such nodes stay nonready until inventory/start attaches the identity. Readiness requires successful authenticated reachability and the committed membership transition. Process started, provider running, open TCP port and `/health/live` are insufficient. Heartbeats carry incarnation, active operations/partition jobs, bounded queue counts and readiness. Expired incarnations lose dispatch/publication authority even if an old process reconnects. All ready workers run the same binary and can coordinate operations and dispatch peer partition jobs; bootstrap has no permanent coordinator role.

API `/health/ready` means the replica has validated configuration and can access the durable control/catalog state; it remains ready for administration at zero workers. A data request with zero serving capacity returns a bounded 503. Worker `/health/ready` means eligible for new dispatch and becomes false on drainage. These checks must not depend on the public data/admin bearer token or return private membership details.

Default timing settings are a 5-second heartbeat, 15-second membership expiry and 300-second launch/readiness deadline, recorded durably. Correctness uses A02's lease/time rules, not comparisons between unsynchronized worker clocks. Launch deadlines fence late registration before replacement. An ambiguous launch remains a reconciliation obligation and counts against provisioning limits until resolved. Drain revokes new operation admission while preserving accepted coordination and partition dependencies. Only verified zero accepted work plus publication fencing authorizes stop_node. Drain timeout leaves the node in draining/error state and never forces provider termination. Graceful API shutdown stops policy changes and relinquishes its controller lease after persisting progress; workers remain alive.

With all callbacks None, serve uses external mode. Operators create bootstrap files with request_id null, configured enrollment credentials and stable node_ids; a new process boot gets a fresh incarnation. Registration stores `external:<node_id>` as the resource identity, with ownership external. No provider callbacks run. Fixed desired count reports an operator capacity goal; `scale` may change that goal but cannot remove or create operator-owned machines. Status states this capability explicitly. Reject tuple nodes and autoscale with unsupported_capability; they would otherwise promise unattended provisioning. Excess external workers can be made unschedulable and drained, but the operator owns process termination. The initial release need not add an infrastructure-provider registry or arbitrary adapter discovery framework.

## Status record

`status()` returns a validated dictionary with exactly:

```text
version: 1
cluster_id: UUID
name: string
revision: positive int
mode: "fixed|autoscale"
desired: int
min_nodes: int
max_nodes: int
capabilities: {provision: bool, terminate: bool, autoscale: bool}
controller: {epoch: nonnegative int, active: bool, degraded: bool}
counts: {ready: int, starting: int, draining: int, lost: int}
nodes: list of {node_id, incarnation: UUID|null, resource_id: string|null,
                ownership: "managed|external", state, operation_slots,
                partition_slots, running_operations, running_partitions,
                queued_operations, queued_partitions, heartbeat_age_seconds}
intents: list of {request_id, node_id, resource_id: string|null,
                  action: "start|stop", state, attempts, error_code: string|null}
metrics_fresh: bool
```

Node state is `starting|ready|draining|lost|stopped`. Intent state is `pending|in_progress|reconciling|completed|failed`; retain completed records for bounded outcome retention, then compact them. Capacity/count fields are nonnegative exact integers; absent heartbeat age is null and otherwise finite nonnegative seconds. Node identity may be known before incarnation/resource identity. Sort node and intent arrays by identity; exclude secrets, provider error text and arbitrary callback output. Counts describe membership states, not a sum of pending callbacks. Provisioning accounting unions launch intents and matching resources by node_id, so a ready worker with a retained start intent is counted once. `metrics_fresh=False` prevents automatic scale-down.

## Lean actual-process acceptance

The proposed runner is `scripts/acceptance_nodes.py --binary server/target/debug/meshdb-server --report PATH`, with optional `--scenario execution|recovery|lifecycle|all`. It uses a fresh owned temporary directory, protected generated credentials and explicit sanitized child environments. It never reads .env, calls AWS, downloads a runtime, builds a binary or silently switches to legacy S3 acceptance. Import and --help are inert. Commands here describe future work and were not run.

Use two Python API supervisors with distinct Rust API children, two identical Rust workers initially, one small independent TCP front door providing a stable ORM URL, and the durable local backend selected by A02. At most one temporary third worker is needed for scale-up/replacement. If the backend is filesystem plus a single control service, document that service as a separate process and a single point of failure; restarting API replicas alone does not prove control-store availability. All operation and partition requests use TCP, including worker-to-worker dispatch. Use operation_slots=2, partition_slots=2 and small queues. Reuse the 96-row deterministic partition fixture with a 64 KB target and require more partitions than workers. Every process has distinct PID/incarnation and private local scratch. Authoritative objects/control state are shared deliberately, and all worker caches are disposable.

Use a test-build-only, loopback authenticated control channel for barriers and bounded event collection. It must be enabled explicitly and absent from normal production configuration. Gates occur immediately after operation-slot admission, after actual partition execution admission, before shared publication, and before readiness/heartbeat transitions. Events contain run/operation/job IDs, node/incarnation, PID, partition reference, sequence, phase and occupancy, never rows/tokens. Gate release requires an explicit harness acknowledgement. Add bounded event queues and a watchdog that fails on abandoned gates. Do not infer remote execution from API logs, client concurrency, a job receipt alone or elapsed duration.

| Scenario | Required causal evidence |
| --- | --- |
| Remote execution and symmetry | Route one operation to worker A and another to B through a test front-door routing gate. For each, require real partition executor entered/completed events on both workers, matching snapshot/object identities and exact query results. The API process has no partition executor. Verify actual Parquet file references, sizes and bounded decoding through the real backend/codec path. |
| More than one query per node | Hold two distinct query IDs after admission on A and require operation occupancy=2 concurrently. Require partition executor entered events for both IDs on A before releasing either partition gate, then real completion/results after release. Hold a third query before operation capacity becomes available and verify bounded queue/overload. Release one operation and observe third admission with occupancy never above 2. Separately run two coordinators that dispatch peer jobs while all operation slots are occupied; partition progress must continue without needing an operation slot. |
| Safe write/retry semantics | Pause competing writes from different coordinators before publication. Observe committed snapshots only, no partial batch and no lost acknowledged write. Drop one response after durable commit, kill its API replica, replay the identical envelope through the survivor and require the same durable outcome with no second publication. A new payload under that ID conflicts. Reuse legacy stale-table-ID and global ordering/tie/limit/projection checks rather than another CRUD suite. |
| API/controller loss | Pause start after durable provider acceptance but before callback acknowledgement, kill the owning supervisor/API, and let the standby acquire a higher epoch. Inventory must recover the same resource/request_id, with exactly one worker resource and no policy reset. Inject a late stale-controller completion and verify it cannot change durable policy or issue a second effective start. Existing workers continue through the stable front door. Restart the killed replica with its original serve nodes argument and require persisted later policy. |
| Worker loss and fencing | Kill a gated worker during a read, then during a mutation preparation/publication boundary in a separate bounded case. Expiry fences the old incarnation; accepted writes are committed once or reported unknown/definitely not applied according to durable evidence. Restart/replacement uses fresh incarnation and cannot publish stale work. All surviving results retain exact baseline types/order. |
| Provisioning lag and drain | Block readiness of an accepted launch. Repeated reconcile ticks must count it once, never as ready and never trigger duplicate starts. Unblock it and verify registration to ready. Hold accepted work on a selected drain victim, request scale-down, verify no new admissions and zero stop callbacks before release. Release all operation/partition work and require stop then confirmed termination. Exercise a drain timeout without forced termination. |
| Autoscale and fixed override | Persist a small min/max policy, gate real query demand to cause growth, and require ready+pending accounting prevents overshoot. Release work, collect fresh zero-work observations across the stabilization period and require safe drain/shrink. Stale metrics suppress shrink. `scale` disables autoscale, survives API restart and never falls below/above the fixed target because of an old observation. |
| External/no AWS | Start workers manually with no callbacks and verify query execution, capability status, fixed operator goal, unsupported autoscale and worker replacement. The same local backend runs without AWS environment variables/CLI/credentials. |

Each case records exact command, topology, configuration capacities, accepted revisions, controller epochs, resource/request identities, ordered gate/event evidence, publication revisions, object checks and observed exit status. Public report data uses local resource aliases and omits bootstrap, bearer headers and raw errors. Fail immediately on a missed causal condition; do not report skipped proof as passed. Real-time polls may wait for heartbeat/stabilization transitions, but correctness assertions depend on observed state and gates, not a chosen sleep duration. Policy arithmetic/clock edge cases belong in focused deterministic control tests rather than many slow process runs.

Reuse `Checks`, `exact`, `safe_failure` and fixture generation directly when their imports are inert. Extend process ownership separately for API/worker commands and explicit local environments; keep the existing S3 helper's behavior intact. Prefer a bound inherited listener or child readiness announcement for port allocation; otherwise retry only bind failure with a fresh port before executing fixtures. No broad refactor of existing runners is necessary.

Cancellation stops future launches and queries, releases test gates, joins clients, gracefully stops and reaps every owned child, and writes the partial sanitized report. Forced kill/nonzero shutdown fails normal acceptance; deliberate crash cases have explicit expected-kill records. Cleanup targets only verified owned process identities and owned temporary state, never arbitrary PIDs or existing storage. Test one startup-failure cleanup path and one cancellation path including report setup, borrowing current guard coverage. On a failed process run retain private diagnostics and a small report; do not print logs that may contain secrets.

## Feasibility and limits

The supplied environment has Docker CLI but a stopped daemon, about 1.4 GiB free, and an existing roughly 5 GiB server/target. Native subprocesses and loopback TCP are the practical first acceptance path. Reuse the existing build directory once implementation is authorized; no parallel Cargo commands, container images or extra virtual environments. The fixture and a few private logs should require only a small bounded amount of disk; cap log/event artifacts and fail preflight if the requested budget is unavailable. No new environment probe was needed for this note.

These processes prove real serialization, TCP boundaries, independent lifetimes, remote partition execution, durable restart behavior, callback identity recovery and bounded concurrency. They do not emulate separate kernels, cross-host filesystems, network partitions/latency, VM boot/cloud quotas, remote secret delivery, power-loss durability or a highly available metadata service. Gates can delay/drop selected network exchanges and exercise ambiguity but do not certify arbitrary partition tolerance. A shared local directory proves only that backend's supported local locking/atomicity, never NFS/object-store guarantees. Future Docker/multi-host runs can extend deployment evidence without changing the SDK. The current implementation cannot run this topology until the local backend and remote runtime are implemented.

## Bounded Python assignments and focused validation

Keep Rust execution/commit/control work with the parent's Rust slices. Split Python ownership so no child receives the whole module:

1. P02a owns a new private control transport and `orm/tests/test_cluster.py`. Implement Cluster connection/argument validation, three wire operations and strict decoding against a small real local HTTP fixture. Group malformed/auth/redirect/body-limit and lost-policy-response cases; reuse existing transport tests instead of copying them all. Export Cluster only after its methods exist.
2. P02b owns only the scale wait loop and its focused tests, after P02a/status routes. Prove accepted-then-deadline, superseded revision and ready-with-pending-work behavior with an injected monotonic clock plus one TCP acknowledgement/poll case. Do not test every trivial accessor or use sleep to prove a deadline.
3. P03a owns serve argument/deployment-config validation and child lifecycle, plus one process cleanup guard. It starts the already-built Rust API with private configuration and leaves all desired-state decisions in Rust. It must preserve SIGINT/report ownership and avoid changing legacy ServerProcess in this slice.
4. P03b owns the bounded callback bridge and one local fake-provider contract group. Prove lost start response followed by inventory recovery, duplicate delivery under one request_id, saturated callback slots and sanitized error handling. This is adapter/bridge evidence, not cluster acceptance.
5. P04 owns only `meshdb worker --bootstrap` forwarding, packaging and bootstrap-file validation handoff. One subprocess argv/exit/signal check suffices; Rust registration/readiness tests remain in R07/C03. No Python reimplementation of the worker.
6. E01a owns a new explicit-environment process helper and the durable local process-provider adapter. One actual tiny-child restart/recovery and cancellation test verifies ownership/registry behavior. Its durable launch critical section must have a deterministic kill point; do not rely solely on mocked Popen calls.
7. E01b owns the lean runner/event collector and execution scenario once Rust test gates exist. E03/E04 add the bounded recovery/lifecycle cases after C04-C07, rather than growing a second acceptance framework.

Parent assigns exact file ownership and resolves shared export/package edits sequentially. Each delivery records focused checks and gets separate specification then quality review as required by the rubric. This design does not create new testing requirements for reversible prose edits.

## Decisions for parent reconciliation before implementation

The Python signatures, callback records, ownership, no-retry semantics, configuration and CLI shapes are fixed by this proposal. Two cross-design details need P01 agreement:

1. A02 must choose the durable control/local object adapters and specify fencing, authoritative inventory completion and retry-outcome retention. Generic `driver/options` records must fail unsupported consistency rather than hide it. This note intentionally does not select an entire new database/consensus implementation.
2. A01 must fix maximum operation/partition slots and queue limits, and provide the test-gate/event schema at the true execution boundaries. Keep separate operation and partition budgets so a coordinating operation cannot consume the capacity its peer jobs require. Acceptance cannot claim this until A01's runtime enforces it.

No other public API decision requires user clarification. The note is a design delivery, and it makes no claim that distributed acceptance has passed.
