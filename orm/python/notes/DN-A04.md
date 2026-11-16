# DN-A04 private serve startup and callback authorization

Design only, 2026-11-10. This note selects the missing private Python/Rust protocol. It does not implement it. No code, builds or tests were run. No applicable `AGENTS.md` was found in the repository or its ancestor directories. Preserve all existing uncommitted work.

Parent reconciliation: accepted for implementation, except the proposed
completed-result cache below is omitted. Python retains only active invocations;
completed results are not cached, and every later invocation requires fresh
authorization plus the provider's durable idempotency contract. Cache-specific
tests are replaced by sequential redelivery/restart evidence using the provider
registry. The shared contract is authoritative. Rust startup/authorization work
is assigned through the existing startup/controller tasks before P03d export.

## Scope and fixed contracts

`plans/distributed-contracts.md` is authoritative over historical A02/A03 proposals. Keep `serve(name, nodes=1, start_node=None, stop_node=None, list_nodes=None)` and the existing callback dictionaries/signatures. Add no public runtime classes, methods or orchestration arguments. Rust owns control storage, policy, membership, leases, intent recovery and all CAS operations. Python starts one API child and executes callbacks. ORM callers and workers acquire no infrastructure responsibilities.

Validate the existing identifier rule, exact integers excluding bool, fixed counts 0..1024 and exact two-element tuples with 1 <= min <= max <= 1024. Callbacks are all three synchronous callables or all absent. Check callable objects' `__call__` as well as ordinary coroutine functions; validate results too. External mode rejects tuples. Initial policy applies only on successful cluster creation; attachment preserves persisted policy even when `nodes` differs.

Read only the path named by `MESHDB_CONFIG`; do not search dotenv files. It names the single versioned JSON config with one `storage` backend. Python validates the path without copying secrets or implementing Rust's config schema. Rust opens/validates the config and protected credential files. Resolve the executable with existing `_binary.resolve_server_binary()`, preserving explicit-override failure and PATH fallback rules. Keep `cli.py`'s implemented worker forwarding unchanged. Use standard-library functions and small private records; do not import the acceptance helper or add provider dependencies.

## Selected startup channel

Use a POSIX `socket.socketpair()` for one inherited, bidirectional startup/lifetime channel. Python passes only the child descriptor number in `MESHDB_SUPERVISOR_FD`, using `Popen(pass_fds=(fd,), close_fds=True, start_new_session=True)`. The argv remains exactly `[binary, "api", "--config", absolute_config_path]`. Remove any inherited variable of that name before setting it. No secret, bridge URL or initial policy goes into argv, environment values, logs, temporary files or the operator config. Parent and child close their unused socket ends immediately; Rust marks its endpoint close-on-exec before doing other work. No worker inherits it.

Each message is four-byte unsigned big-endian length followed by exactly that many UTF-8 JSON bytes. Frames are nonempty and at most 16 KiB, checked before allocation. Objects reject duplicate/unknown fields, nonfinite numbers, wrong exact types and unsupported versions. EOF during a frame is failure. Startup has a single monotonic 30-second budget; each partial read/write consumes that budget. After startup, use the same bounded framing, with a two-second frame assembly deadline once a frame begins. An idle lifetime channel has no read timeout.

The exact initial messages are:

```text
Python -> Rust
{version:1, type:"init", name:identifier,
 initial_policy:{mode:"fixed"|"autoscale", desired:int, min_nodes:int, max_nodes:int},
 provisioning:"managed"|"external",
 bridge:null|{url:"http://127.0.0.1:PORT/private/v1/callback",
              callback_token:secret, authorize_token:secret}}

Rust -> Python
{version:1, type:"prepared", cluster_id:UUID,
 bound_url:absolute_HTTP(S)_URL,
 authorize_url:null|"http://127.0.0.1:PORT/private/v1/authorize"}

Python -> Rust
{version:1, type:"activate"}

Rust -> Python
{version:1, type:"ready", cluster_id:UUID, bound_url:absolute_HTTP(S)_URL}
```

Fixed policy sets both bounds equal to desired; autoscale starts at its minimum. External mode uses `bridge:null` and `authorize_url:null`. Every UUID uses canonical lowercase spelling. Tokens are independent 32-byte random values encoded as unpadded base64url, created by Python for this child lifetime only. They have no admin/data/enrollment/worker authority. Keep them in memory; never include them in exception text or object reprs. The private sockets and local user account are the trust boundary, not protection against a compromised same-user process.

Rust may send `{version:1,type:"fatal",code:CODE}` at any startup stage, then exit. Allowed codes are `invalid_config`, `incompatible_cluster`, `storage_unavailable`, `bind_failed`, `protocol_error`, `startup_failed`. No paths, provider messages or configuration values accompany them. Unknown/malformed messages fail closed as `protocol_error`. Public argument/path failures use existing `ValidationError`; startup/runtime failures use a sanitized existing `MeshDBError`, with no raw chained exception. SIGINT retains normal `KeyboardInterrupt` behavior after cleanup.

Python binds its managed bridge to `127.0.0.1:0` before spawning. It starts bounded request handling, but returns `503 bridge_not_active` until activation. Rust binds the configured API listener and, in managed mode, a separate private authorization listener at `127.0.0.1:0`. Rust validates storage/configuration and creates or attaches control state before `prepared`. It does not acquire controller authority or dispatch callbacks yet. API readiness stays false. Python checks the announced addresses, installs the authorization target, opens bridge admission, and sends `activate`. Rust starts controller reconciliation and API admission, then sends `ready`. Ready means API/controller machinery is runnable; it never waits for a provider callback, node readiness or desired fleet convergence.

The authorization listener runs independently of the controller task. It never needs a controller mutex held across callback HTTP I/O. The controller drops control locks before invoking Python; reads and renewals continue while it awaits a callback. These rules remove both the startup dependency cycle and callback/authorization deadlock.

`bound_url` comes from the actual listening socket, including its allocated port, and uses configured TLS. Acceptance binds to `127.0.0.1:0`; it must not reserve and release a guessed port as `scripts/server_process.py` currently does. After `ready`, Python emits one bounded JSON line to stdout, `{ "event":"meshdb.serve.ready", "bound_url":URL }`, with no credentials, identities or config. This is a private process announcement, not a new Python return value or public method. The owning acceptance process drains stdout before startup and uses the announced address. Rust does not rewrite `advertise_url` or `api_urls`: acceptance supplies a stable front-door URL there, bound before launching APIs, and routes it to the announced sockets. Reject port-zero advertised URLs. A bind wildcard is not a usable connection URL; acceptance uses the explicit loopback bind. Any write failure on the announcement takes the startup-failure cleanup path.

After ready, the only parent message is `{version:1,type:"shutdown"}`. Rust acknowledges completion by process exit, not by a promise before exit. Parent EOF triggers Rust API shutdown and controller relinquishment, never worker termination. Unexpected child channel EOF is a supervisor failure even if the process has not yet exited. Neither side writes logs to this socket. Child stdout/stderr must go to a continuously drained, size-capped private diagnostic sink, never an unread PIPE or unsanitized forwarded stream.

## Callback and authorization wire records

Both private HTTP listeners accept only the exact POST route above, over numeric IPv4 loopback, with `Authorization: Bearer TOKEN`. Use the callback token Rust-to-Python and the distinct authorization token Python-to-Rust. Compare credentials in constant time. Reject other peers/routes/methods, redirects, URL userinfo and non-loopback destinations. Do not use environment proxies. Request headers are at most 8 KiB, JSON bodies and responses at most 1 MiB. Require Content-Length and application/json, reject transfer encoding and content encoding, use one request per connection, and enforce an absolute two-second header/body assembly deadline. Apply startup's strict JSON rules here too, with nesting depth at most 32. Bound response writes to two seconds after the result is available. Errors never echo input.

Rust serializes this command once and retains its exact bytes for the attempt:

```text
{version:1, invocation_id:UUID, cluster_id:UUID,
 owner_id:UUID, epoch:u64_positive,
 intent_id:UUID|null, action:"start"|"stop"|"inventory",
 target:null|{node_id:UUID, request_id:UUID, incarnation_id:UUID|null},
 payload:OBJECT}
```

For start, `intent_id` equals the durable provisioning request ID, target has that request/node and null incarnation, and payload is exactly A03's `start_node` request. For stop, `intent_id` is the durable stop command ID, target names the original launch and its incarnation when known, and payload is exactly `{resource_id:STRING}`. Stopping a never-registered resource may have a null incarnation, but requires the persisted exact resource/launch binding. Inventory has null intent/target and `{}` payload; its invocation ID is a transient inventory round. `invocation_id` identifies one delivery attempt and is never a replacement provisioning identity. Command target, action and payload must match the persisted intent; callbacks themselves receive only their fixed public arguments.

After acquiring a callback slot, its worker POSTs the original command bytes to Rust's authorization route. There is no Python storage client or CAS. Rust independently reads a fresh control root through the configured backend, validates its own active renewable lease, checks the supplied owner/epoch, and matches the exact durable intent, eligible phase, target and payload. This includes bootstrap identities/template/credentials for start and exact resource binding for stop. Inventory checks current controller authority without inventing a durable inventory lifecycle intent. On incomplete reads, uncertain ownership or local renewal failure it denies invocation. Takeover CAS, rather than a JSON deadline alone, remains A02's fencing point.

The exact authorization success is:

```text
{version:1, invocation_id:UUID, command_sha256:LOWERCASE_HEX_64,
 authorized:true}
```

The digest covers the exact received command bytes, with no cross-language JSON canonicalization. Rust both validates fields against persistence and hashes those bytes. Python verifies the response identity/digest and calls the callback immediately in that same worker, without another queue, asynchronous handoff or cached authorization. Authorization has a two-second total deadline; there are no automatic retries. The response is single-attempt evidence of a fresh ownership read, not a reusable ticket or claim of atomic fencing of provider effects. Takeover may still happen between authorization and the provider effect. Already-started or late effects require provider idempotency and exact resource identity. Rust must adopt pending/ambiguous identities and reject stale completions through its existing owner/epoch CAS rules.

Authorization failures use `{version:1,invocation_id:UUID,error:{code:CODE}}`. `401 unauthorized`, `409 ownership_changed` and `503 ownership_unavailable` deny invocation. Invalid commands use `400 invalid_command`; oversize uses `413 limit_exceeded`. Pre-parse/authentication errors have null invocation_id. Never accept an authorization failure as an empty success or recover it from a cached lease snapshot.

Callback responses are exactly one of:

```text
{version:1, invocation_id:UUID, result:VALUE}
{version:1, invocation_id:UUID,
 error:{code:CODE, outcome:"not_invoked"|"unknown"}}
```

A 200 result is the validated resource string for start, null for stop, or the complete A03 inventory list. Require stop to return None. Inventory has at most 4096 records, fits the encoded limit and rejects unknown/missing fields, wrong exact types, invalid UUIDs/states, duplicate resources and duplicate active launch identities. Validate using the fixed A03 records; never truncate or include partial inventory. Awaitable results are invalid, and a returned coroutine may be closed without running it. Do not iterate arbitrary generators or consume unbounded iterables. Callback exceptions, invalid results and inventory conflicts produce fixed codes, never exception messages, reprs, tracebacks or raw provider data. Inventory from another cluster is invalid. Unknown resources with valid records remain Rust's reconciliation responsibility.

Use `400 invalid_command`, `401 unauthorized`, `409 callback_in_progress` or `identity_conflict`, `413 limit_exceeded`, `429 callback_capacity`, `503 bridge_not_active`, `503 ownership_unavailable`, `409 ownership_changed`, `502 callback_failed` or `invalid_callback_result`, and `504 callback_timeout`. Before invocation, outcome is `not_invoked`; after entry or uncertain delivery, start/stop outcome is `unknown`. Malformed inventory yields no usable observation. Rust treats transport loss after dispatch as unknown regardless of Python's possible local classification, persists sanitized error codes and controls backoff/recovery. Success is not a durable control update until Rust accepts it under the matching owner/epoch/intent.

## Finite callback ownership

Create exactly four daemon callback threads per process runtime: three action workers and one inventory worker. Each has a single handoff mailbox, accepts work only while idle, and keeps its slot from authorization through callback return. There is no pending callback queue. Inventory cannot borrow an action slot and actions cannot borrow the inventory slot. Observation expires 30 seconds after admission, including authorization time; Rust's HTTP deadline is 35 seconds total. A callback that has not entered when its observation expires must never enter later. Coordinate that check and entry state under a short local lock.

At observation expiry, send `callback_timeout` and retain the occupied slot until the function actually exits. Do not cancel its future, reuse the thread or start a replacement. Late completion can populate the bounded result cache but never pushes an unsolicited durable update. Rust can reconcile inventory or redeliver the same identity. If all three actions hang, inventory still runs; if inventory hangs too, managed reconciliation degrades until the adapter returns or the host is restarted. Data HTTP remains Rust's independent responsibility.

Bound HTTP handling separately to eight fixed daemon handler threads, eight queued sockets and listen backlog eight. Header/body deadlines and immediate rejection of saturated callback admission prevent an unlimited population of waiting handlers. At most three handlers wait on actions and one on inventory; slow/malicious headers can delay inventory until their two-second assembly deadline. No `ThreadingHTTPServer` with an unbounded thread per connection, unbounded executor queue or replacement pool. Shutdown closes listening and queued/active request sockets without trying to kill executing callback functions.

Before assignment, deduplicate active work by `(cluster_id, action, intent_id)`; inventory uses invocation_id. An identical active delivery returns `callback_in_progress/unknown` and never runs again concurrently. A different immutable target/payload for the same intent returns `identity_conflict`. Keep at most 256 completed action results and 4 MiB total cache bytes, for at most 60 seconds, evicting oldest completed entries only; keep the at-most-four unfinished entries until return. Do not cache inventory results. Before serving a cached successful result, authorize that incoming attempt too, with bounded handler I/O independent of callback slots. Echo its invocation_id. Do not bypass ownership checks with the cache. Cache misses after expiry/restart can invoke again only after fresh Rust authorization; provider idempotency supplies durable duplicate safety. This cache makes no exactly-once claim. Never persist bootstrap or provider results in Python.

Reject concurrent `serve` runtimes in one process. Retain the private runtime guard after return while any callback thread remains alive, so repeated calls cannot accumulate replacement pools. Allow a new runtime only after all old threads have exited. This restriction also applies after startup failure. Retain only the necessary in-memory state, without claiming that Python can scrub secrets captured by a hung user callback.

## Shutdown ownership and limits

`serve` blocks until shutdown/child exit and always waits/reaps its owned Rust child. On main-thread entry, save SIGINT/SIGTERM handlers and temporarily install handlers that only record shutdown and wake the supervisor. On first signal, close callback admission, send shutdown and forward that signal to the verified owned child if still alive. Also use this path on startup failure, channel failure, caller exception or child exit. Never signal worker resources or call stop_node as API teardown. Children run in a separate session so terminal SIGINT is not accidentally delivered twice to their process group.

Allow ten seconds for child exit, then SIGKILL only that child and wait/reap. Report forced or unexpected nonzero exit as sanitized failure; do not call it graceful. A child's expected exit from the forwarded first signal follows the saved-handler behavior below instead. If the OS cannot confirm exit after killing, keep ownership and report the cleanup failure; there is no honest bounded guarantee for uninterruptible kernel waits. Close both channel ends, all owned sockets and diagnostic resources in finally paths, including Popen failure before a PID exists. Rust independently monitors supervisor EOF, stops API admission, abandons callback waits and relinquishes controller authority without changing desired policy or terminating workers.

Wait at most two seconds in total for callback threads during teardown. Daemon threads may remain alive and produce external effects after serve returns; Python cannot safely kill arbitrary synchronous callbacks. Rust still fences metadata completions. Provider code may itself create non-daemon threads or subprocesses outside supervisor ownership, so prompt interpreter exit cannot be promised for arbitrary adapters. Do not join a standard executor whose shutdown would wait forever.

Restore saved signal handlers in finally. After cleanup invoke a saved callable handler once for the first signal, preserving its usual exception behavior; default SIGINT raises KeyboardInterrupt, default SIGTERM returns None from serve, and saved SIG_IGN is restored without dispatch. Handler restoration must not overwrite a handler deliberately replaced by the caller while serve ran. A second signal expedites child kill/reap, rather than starting recursive teardown.

Non-main-thread serve installs no signal handlers. It blocks until child exit or its host process ends, and offers no public cross-thread cancellation handle. The embedding application owns process signals. A KeyboardInterrupt in the main thread does not automatically cancel this call. Parent process death closes the private socket and lets Rust shut down; SIGKILL cannot run Python cleanup or reap its own child. State these limits rather than promise thread cancellation or process-death cleanup by Python finally blocks.

## Subsequent bounded assignments

Run four small GPT-6.1 Sol high tasks under the parent, with sequential integration and no shared file owners. These are future assignments, not delegation performed here.

| Assignment | Exact Python ownership | Completion evidence |
| --- | --- | --- |
| P03a validation | New `orm/src/meshdb/_serve_args.py`, `orm/tests/test_serve_args.py` | Public arguments/callback declarations and MESHDB_CONFIG path validation only. No duplicate backend/auth/config parser, subprocesses or exports. |
| P03b process | New `orm/src/meshdb/_serve_process.py`, `orm/tests/test_serve_process.py`, `orm/tests/support/serve_child.py` | Resolver reuse, inherited startup framing, port-zero announcement, signal ownership, failure teardown and reaping against actual tiny subprocesses. Private functions accept already-bound bridge data; no exported serve or infrastructure fallback. |
| P03c bridge | New `orm/src/meshdb/_callback_bridge.py`, `orm/tests/test_callback_bridge.py` | Fixed callback threads, bounded ingress, actual loopback authorization, identity checks/cache, sanitization and honest saturation. Tests start their own small authority fixture in this test file, without editing the process fixture. |
| P03d complete integration | New `orm/src/meshdb/serve.py`, `orm/tests/test_serve.py`; only this assignment edits `orm/src/meshdb/__init__.py` | Compose validation, bridge and child, with signal/failure cleanup across all paths. Export serve only after the full path works against Rust. Preserve existing shared exports and CLI work. No stubbed public function while Rust support is missing. |

The parent must assign the matching Rust dependency to its existing startup/controller owner: `server/src/main.rs` startup wiring and the private runtime/control module selected by that owner. This note does not authorize Python tasks to edit Rust or open another controller implementation. Recommendation: implement the selected FD handshake and independent Rust authorization listener before P03d. That coordinated Rust wire commitment is the only parent decision needed here; public interfaces and callback signatures remain settled.

Keep focused tests causal and small. Process tests use actual POSIX children and framed events to prove partial startup failure, EOF, actual port-zero binding, SIGINT/SIGTERM forwarding, child exit status/reaping and no surviving owned child. A gate-aware child must hold shutdown until explicitly released, so a startup-success-only mock cannot pass. A short configured private test deadline may test forced kill; do not add a public timeout knob.

Bridge tests send real TCP commands to the bridge and run a real loopback authorization fixture. Gate authorization, change the fixture's owner/epoch/intent before releasing it, and require zero callback entries. Separately let authority serve requests while its simulated controller awaits the callback, proving the channel has no dependency cycle. This proves Python wire behavior, not actual storage lease correctness.

For duplicates/ambiguity, gate after provider acceptance but before response. Redeliver and require one concurrent invocation, timeout/retain its slot, and recover the same resource through inventory. After completion, exercise cache expiry or a fresh bridge with a provider registry that deduplicates the same provisioning request; require the same resource. Release a previously authorized callback after an epoch change and record its possible late effect. Python must not claim it prevented that effect or accepted a durable stale completion. Malformed inventory, exceptions and awaitables yield only fixed sanitized errors.

For saturation, use three action-entry events and one inventory-entry event, each held by an explicit gate. Require overload on the fourth action, inventory progress before its own gate blocks, at most four active callbacks and no new callback threads across repeated observation timeouts. Release gates in finally and prove occupied slots become usable only on actual return. Teardown with a held callback must reap the API child within its shutdown contract while documenting the surviving daemon/guard. Use bounded event waits and failure watchdogs, never sleep-based overlap assertions.

P03d must include one real built Rust API plus serve-host process over the local conditional backend, using the actual authorization listener, not only the authority fixture. Confirm ready announcement, preserved attached policy, command authorization and signal cleanup without automatically provisioning cloud resources. Rust's focused control tests prove persisted ownership checks and stale completion rejection. Existing planned E03/E04 then prove durable takeover, lost callback responses, late inventory recovery and full worker lifecycle with actual APIs/workers/local store. Tiny children and fake-provider gates establish component behavior only; none of these substitutes for real-cluster acceptance or proves separate-host/cloud behavior.
