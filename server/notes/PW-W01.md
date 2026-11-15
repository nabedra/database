# PW-W01 dedicated worker pool

Owned files:

- `server/src/worker_pool.rs` (new)
- `server/src/lib.rs` (only added `pub mod worker_pool;`; preserved existing partition_size export)
- `server/tests/worker_pool.rs` (new, two grouped tests)
- `server/notes/PW-W01.md` (new)

No Engine, runtime, main, storage, queries, coordinator plan, dependencies, branches, commits, pushes, or secrets were changed.

## API and integration contract

```rust
pub type WorkerId = usize;
WorkerPool::start(NonZeroUsize, tokio::runtime::Handle)
    -> Result<(WorkerPool, WorkerThreads), PoolStartError>;
WorkerPool::worker_count(&self) -> NonZeroUsize;
WorkerPool::submit<T, F>(&self, partition_index: usize, work: F)
    -> impl Future<Output = Result<Job<T>, ProtocolError>>;
// T: Send + 'static
// F: FnOnce(WorkerId, &tokio::runtime::Runtime)
//      -> Result<T, ProtocolError> + Send + 'static
Job<T>::receive(self) -> impl Future<Output = Result<T, ProtocolError>>;
WorkerThreads::shutdown(self)
    -> impl Future<Output = Result<(), ProtocolError>> + Send + 'static;
```

Public exports support internal library wiring/integration tests, not a transport API. WorkerPool clones share dispatch state. Startup must retain the separate, noncloneable WorkerThreads owner outside worker captures. Exactly N persistent std threads are named `meshdb-worker-{id}`, with IDs in `0..N` and one enabled current-thread Tokio runtime each. Original partition_index modulo N selects the worker. Closures run directly outside block_on on their assigned thread, using the supplied runtime.block_on for storage IO and synchronous decode/evaluate/encode afterward. No data work uses spawn_blocking.

Each worker has a queue of capacity one, allowing N executing plus N queued jobs. Submission waits for a reservation without holding the state lock, then checks admission and transfers the job under the same short lock used to stop admission. Stop wakes reservation waiters and workers close their receivers, so an unpolled, unaccepted reservation cannot hold shutdown open. Already accepted queued work drains. Dropping a Job receiver does not cancel execution. Closure panics, including panics inside its block_on, return sanitized InternalError/NotApplied and leave the same worker available. Closed result channels also map to sanitized InternalError/NotApplied. No retries or replacement threads exist. Rust's process panic hook remains unchanged; the protocol error never includes the panic payload.

shutdown initiates stopping synchronously before returning its awaitable. One coordinator spawn_blocking lifecycle task awaits every worker's drained signal, keeping all exit gates alive. Each drained worker continues polling its runtime until all workers drain. The lifecycle task then releases every gate and joins every thread, including after a drain/join error. Dropping the shutdown awaitable does not cancel this lifecycle task. Fallback owner Drop stops admission and schedules the same cleanup without blocking or joining on the dropping thread. The pool state never owns the lifecycle owner, so a last pool/Engine-like dispatch Arc can safely drop in a worker without a lifetime cycle.

Startup allocates incrementally as threads start, with no eager allocation based on an untrusted count. Runtime creation occurs on each worker with a startup handshake. Thread/runtime creation failure stops and joins all partially started workers, returning only PoolStartError { worker_index }, without OS/runtime error details.

Next wiring must retain the coordinator runtime until cleanup completes, keep WorkerThreads in startup, drain requests before pool shutdown, and avoid nested submit/wait calls from worker jobs. Keep each operation's window at most N and join every admitted job through errors/client disconnects before releasing request or mutation state. Those operation-level policies belong to the wiring tasks. No Engine integration was added here.

## Checks and evidence

All Cargo commands ran sequentially, using the existing server/target:

```sh
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo test --manifest-path server/Cargo.toml --test worker_pool
rustfmt --edition 2024 --check --config skip_children=true server/src/worker_pool.rs server/src/lib.rs server/tests/worker_pool.rs
git diff --check -- server/src/worker_pool.rs server/src/lib.rs server/tests/worker_pool.rs server/notes/PW-W01.md
```

Initial red: exit 101 because the worker_pool module did not exist. This was an API compilation failure rather than a runtime assertion failure. Initial green: both grouped tests passed. Strengthened shutdown coverage then reproduced a real failure: shutdown hit the 15-second hang guard while an unpolled reservation held a sender. Worker-side receiver closure fixed it; both tests passed again. Final coverage adds cross-worker IO polling, thread-exit evidence, and fallback owner Drop. Final targeted run: 2 passed, 0 failed. Owned formatting and whitespace checks passed.

The two grouped tests cover three workers/seven jobs, first-three overlap, distinct and stable OS ThreadIds before/after an async gate and synchronous work, worker names, closures outside Tokio runtime context, original-index modulo routing, bounded queue admission, dropped result receivers, a panic followed by successor work on the same OS thread, sanitized error/outcome, synchronous rejection at shutdown, a pending shutdown until gates release, all accepted nonpanic completions, unpolled reservation independence, worker thread-local destruction before explicit shutdown returns, drained-runtime IO needed by another worker, last dispatch Arc dropping within a worker, and nonblocking fallback Drop followed by worker exit. Timeouts only guard hangs; no sleeps or timing-based overlap assertions are used.

Startup OS/runtime failure and unexpected infrastructure thread failure are handled by inspected cleanup paths, without resource-exhaustion/fault-injection tests. The process panic hook may log a caught job panic under the caller's logging policy. Fallback cleanup requires a live coordinator runtime as documented. No remaining pool blocker is known. No full suite, Clippy, or AWS checks ran.
