# PW-W01b Engine injection and bootstrap lifecycle

Changed files for this assignment:

- `server/src/engine.rs`: constructor, clone, fields, accessors only. Existing catalog/partition representation edits were already present and preserved.
- `server/src/main.rs`: bootstrap injection and explicit awaited lifecycle cleanup.
- `server/tests/runtime.rs`: focused wiring and shutdown behavior.
- `server/tests/startup.rs`: validation overflow case and malformed TLS with explicit worker/partition options.
- Mechanical Engine fixture migrations in `server/tests/api.rs`, `server/tests/health.rs`, `server/tests/engine_catalog.rs`, `server/tests/engine_dispatch.rs`, `server/tests/engine_read.rs`, and `server/tests/engine_write.rs`.
- `server/notes/PW-W01b.md`: this handoff.

`server/src/runtime.rs` needed no change. No CRUD/query execution changes, dependency changes, coordinator plan edits, branches, commits, pushes, agents, credentials, or environment files were involved.

## API and lifetime

```rust
Engine::new(storage: Storage<B>, worker_pool: WorkerPool, partition_size: PartitionSize) -> Engine<B>
Engine::worker_pool(&self) -> &WorkerPool
Engine::partition_size(&self) -> PartitionSize
```

Clones retain the injected size and share the same pool dispatch state and mutation coordinator. Engine never owns WorkerThreads and never starts a replacement/default pool. Later partition modules can use the public accessors.

Bootstrap completes configuration, native TLS parsing, signal registration, storage/catalog initialization, and request Executor construction before starting exactly the configured number of OS workers through WorkerPool::start. Engine receives that handle and the validated partition size. Request concurrency remains a separate Executor budget, currently set from the same configured N.

Bootstrap holds WorkerThreads across runtime::serve. Every ordinary path after successful pool startup reaches awaited shutdown on the still-live coordinator runtime. Listener bind failure drains the Executor; runtime::serve already stops admission/ingress and drains requests on both success and errors. Only after that does bootstrap stop/drain/join workers. The inner async result block confines early returns so they cannot skip cleanup. Both the serve result and worker shutdown are awaited; result.and(shutdown) preserves a listener error when both fail. Worker startup and shutdown errors use static sanitized diagnostics. Existing TLS/listener handling is preserved.

Every migrated fixture returns the separate WorkerThreads owner and each test retains it until requests finish, then explicitly awaits shutdown. Health fixtures release backend state guards before shutdown. No helper introduces a hidden pool on clone or per request.

## Checks and evidence

All Cargo commands ran sequentially against existing server/target, with CARGO_BUILD_JOBS=2, CARGO_PROFILE_DEV_DEBUG=0 and CARGO_PROFILE_TEST_DEBUG=0.

- Red: `cargo test --manifest-path server/Cargo.toml --test runtime` exited 101 because the new constructor arguments and worker_pool/partition_size accessors did not exist. This was an API compilation failure, not a runtime assertion failure.
- Initial green: `cargo test --manifest-path server/Cargo.toml --test runtime --test startup`, 2 runtime and 1 startup tests passed.
- All test targets compiled with `cargo test --manifest-path server/Cargo.toml --tests --no-run`.
- Final strengthened run: `cargo test --manifest-path server/Cargo.toml --test runtime --test startup`, 2 runtime and 1 startup tests passed.
- After releasing health fixture guards before shutdown, `cargo test --manifest-path server/Cargo.toml --test health --no-run` also passed.
- Owned rustfmt checks with edition 2024 and skip_children=true, plus owned git diff --check, passed.

The wiring test injects three workers and a nondefault 7 KiB size, compares original/clone OS thread IDs and original-index modulo mapping across seven indices, checks coordinator sharing and size retention, then verifies owner shutdown closes all dispatch handles.

The lifecycle test uses the existing Fake/ReadGate fixture and actual runtime::serve. A real HTTP health response establishes ingress. An admitted detached operation runs a gated storage read on its injected worker runtime. Its result receiver is dropped, modeling a disconnected response consumer. A queued admitted operation submits a second pool job only after the first finishes, proving pool admission stays open throughout Executor drain. Shutdown closes ingress while work is gated, waits for both operations and explicit worker teardown, and leaves both request/pool admission closed. It covers successful shutdown signals and a shutdown-future error. Timeouts guard hangs; no sleeps are used.

Startup tests verify local validation and native malformed-TLS diagnostics without AWS. Real listener faults and OS worker startup failure were inspected rather than fault-injected. The lifecycle test drops the Executor response receiver; it does not claim a new end-to-end HTTP disconnect test. No full suite, Clippy, or AWS checks ran.

## Pending work

Partition read/mutation execution is intentionally unchanged, including existing raw spawn_blocking calls. Later tasks must route that work through the injected pool, apply the size cap, enforce bounded per-operation windows, and await every admitted job through errors and disconnects before releasing request/mutation state. No remaining injection/lifecycle blocker is known.
