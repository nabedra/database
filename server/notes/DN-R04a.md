# DN-R04a bounded local admission owners

Delivered only adopted DN-A20 mini 1 on shared main. The three-file delta is
new `server/src/node/admission.rs`, one private `mod admission;` line in
`server/src/node/mod.rs`, and this new note. The original module file was
copied outside the checkout to `/tmp/meshdb-R04a-node-mod-before.rs` before
editing. Existing uncommitted work is preserved. No delegation, branches,
worktrees, commits, pushes, dependencies, downloads, target cleanup, network,
cloud, profile, .env or real-secret reads were used.

Guidance included the distributed contracts and R03/R04/F04/C06/testing and
orchestration rubric, adopted A20 common rules/mini 1, A07/A06/A09/A15/A18/A16/A05,
reviewed R01b2b3a/b/c and R02a1/a2/a3 delivery notes, and DN-test-lints.md.
Applied TDD and unslop. No repository or workspace-ancestor AGENTS.md was found;
the discovered `/Users/antoniopenapena/.codex/AGENTS.md` was empty. Inspected
actual private auth eligibility/binding/boot getters, full OwnershipObservation
equality/revision and member capacity validation. No R03a3c reply-handoff API is
used. A21 was proposed when this assignment began and was adopted during the
work; its later C03/R04c receipt amendments do not extend mini 1 and are not
implemented here.

## Private API and retained ownership

Everything in admission.rs is private, with item-specific documented pending
R04b/c dead-code allowances. There is no final authenticated accept API.

- `LocalAdmission::new(cluster, key, coordinator, initialization,
  operation_workers, partition_workers, queue)` checks the coordinator public
  key against the actual WorkerSigningKey and constructs ReceiverIdentity from
  its full node/incarnation/session tuple. Both worker counts independently
  require 1..=256; queue requires 0..=4096. It preallocates O and P optional
  record positions, each workers+queue <=4352, and exactly 4096 dependency
  positions. Binding coordinator Arcs are prepared outside the future lock;
  record duplication clones only an Arc and fixed identity values.
- `Binding::from(&DependencyBinding)` copies the existing exact checked getters
  outside the lock. Binding retains coordinator including key, operation,
  admission and digest. `reserve_operation(binding, admission, self_permit)`
  requires matching admission candidate and exact local coordinator, and reserves
  operation plus self dependency together. It acquires no partition capacity.
  `open_dependency(binding, permit)` converges only for an exact open record.
  Changed coordinator node/incarnation/session/key or binding conflicts; closing
  cannot reopen. Fresh opens require initialized Ready and an open barrier.
- `reserve_partition(binding, permit, job, attempt)` atomically installs an owned
  use against an exact open dependency. It uses partition capacity alone and
  may accept a matching existing permit under permanent drain. Closing refuses
  new uses. Retained duplicate job/attempt identities conflict; this supplies no
  cross-attempt retry policy or durable deduplication.
- `WorkKey` and `DependencyKey` include slot and checked monotonic generation.
  Reservation checks capacity, dependency room, collisions and arithmetic before
  installing anything. One operation consumes two generation values, a new open
  or partition consumes one. Overflow refuses without wrapping or mutation.
- `Capacity`, `WorkRecord`, `WorkPhase`, `DependencyRecord`, `DependencyUse`,
  `Occupancy` and `LocalSnapshot` represent actual local owners. RunningReserved
  already charges a worker; Queued charges only its separately bounded queue.
  `next_start(kind, StartCheck)` checks exact retained key/generation/binding,
  dependency, boot and complete current root observation, then promotes only
  with a free worker charge. A Closing dependency preserves an accepted use.
  Failed checks preserve the record and charges. No semaphore or waiter list
  represents these counts.
- `execution_settled`, `delivery_settled`, `cleanup_settled`, then `reap` are
  concrete ordered transitions. Execution includes settled dispatch cancellation;
  delivery includes operation publication. Charges survive all phases until
  reaping. Ticket/observer disappearance never runs these transitions.
  `begin_close` returns Pending until retained operation/use owners reap;
  each use remains through its delivery and cleanup phases. Operation delivery
  closes its self dependency, and operation reaping removes only that hold.
  Accepted self jobs can keep the dependency alive afterward. Absent Closed is
  explicitly not historical remote-completion evidence. There is no timeout,
  heartbeat-loss eviction, TTL, tombstone or lost-coordinator cleanup.
- `observe(DurableView)` orders complete typed root observations. Equal-revision
  disagreement fails the boot closed permanently, preserving owners.
  `observe_drain(exact_boot, drain)` permanently closes operations/new opens;
  identical drain converges, a different drain conflicts, and later Ready
  cannot reopen it. Typed durable Draining closes the barrier before drain HTTP.
  Denied association blocks execution and admission without freeing owners.
- `InitState` retains one initialization UUID beginning Uninitialized. No
  production completion method or success flag exists. `readiness(boot,
  &Readiness)` reads initialization, barrier and exact current durable probe
  eligibility and echoes the existing decoded typed challenge. Starting/registered
  may answer after initialization, but cannot admit operations. Readiness never
  initializes or changes durable Ready.

`DurableView` and move-only `StartCheck` have private fields and no production
raw constructor. The private fixture alone issues them, using validated Control
roundtrips, complete OwnershipObservation, exact key verifier commitments and
concrete membership phases. These receipts do not establish fresh HTTP authority.
The production issuer is R04c's responsibility and must use associated fresh
eligible/content proofs, including complete coordinator eligibility and retained
job identity. Candidate UUID generation, encoding, hashing and binding Arc
preparation belong outside the receiver critical section.

## Local tests and provenance

Ten grouped tests use real local records and lifecycle gates with synchronous
bounded calls. No threads/tasks, sleeps, detached cleanup or storage are needed.
The occupied RunningReserved record is the gate holding each accepted queue;
explicit settlement/reaping releases it before a promotion can succeed. Tests
settle retained records explicitly. These gates prove local state transitions,
not registered payload supervisors or worker execution.

Coverage includes independent operation/partition workers and zero/nonzero
queues; next-owner refusal and reuse only after reap; both worker-count bounds;
atomic operation+self rollback at the real 4096 dependency limit including self
and closing entries; exact versus mismatched/closing retries; both sides of the
barrier ordering; a distinct peer key and coordinator identity; accepted self and
peer jobs during drain; Closing queue promotion; self retention after operation
reaping; Pending through delivery/cleanup and actual local reaping; an accepted
queued owner refused by independently validated newer root and changed boot or
session authority; durable drain before HTTP and missing/lost association;
stale generation, checked exhaustion, initialization identity and typed readiness.

Two focused behavioral reds were personally observed:

1. Before implementing capacity validation, the constructor scaffold accepted
   workers=0. `independent_capacity_bounds` failed its intended assertion,
   exit 101, after 29.57s build and 0.00s test.
2. After initial green, `root_integrity_failure_is_sticky` showed an exact
   valid readiness baseline, then a same-revision conflicting checked root.
   Readiness incorrectly remained Ok instead of Ineligible. Exit 101, 36.19s
   build and 0.01s test. Sticky failure handling fixed this.

The first full-core compile failed because existing Sender deliberately lacks
Debug. The local Binding debug implementation now redacts its fields; no auth
change was made. That was a compilation failure, not behavioral red. Other
initial ownership assertions were green-only evidence. The initial eight groups
passed, followed by nine and ten after adding stronger distinct-peer/self-owner,
initialization/probe and durable-association checks. Those later assertions are
not represented as separate failing-before evidence. The first strict Clippy
run found two local collapsible-if diagnostics; both were fixed without lint
suppression. A subsequent strict lib/tests run passed in 27.00s before the final
typed Readiness adapter; final checks below cover the final source.

## Verification

Every Cargo invocation used the existing server/target and:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

Every invocation used `--manifest-path server/Cargo.toml --locked --offline
--no-default-features`. Exact command forms, including the focused reds above:

```text
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission::tests::independent_capacity_bounds -- --exact
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission::ownership_tests::root_integrity_failure_is_sticky -- --exact
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::admission
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib node::auth::verify::eligibility::tests
cargo clippy --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- -D warnings
rustfmt --check --edition 2024 --config skip_children=true server/src/node/admission.rs server/src/node/mod.rs
```

Final source results, all exit 0:

- Admission: 10 passed, 0 failed, 205 filtered; 36.42s compilation, 0.54s tests.
- Existing auth eligibility regression: 7 passed, 0 failed, 208 filtered;
  1.46s build, 0.03s tests. This exercises the exact boot/binding, current phase
  and complete observation APIs reused here, without a redundant crypto matrix.
- Final strict Clippy library/tests: 22.82s, no warnings or new unowned diagnostics.
- Owned rustfmt check passed. Scoped git diff --check and direct whitespace and
  single-final-newline checks passed for all three files, including untracked
  files. The module file exactly equals its external snapshot plus the one
  private declaration. No existing lint backlog is treated as open.

The final diff is 1302 new Rust lines including fixtures/tests, one module line,
and this note. The original module file and all unrelated changes remain
otherwise intact. Approximately 2.2 GiB free disk was observed during this work;
no alternate target or cleanup was used.

## Future integration and limits

R04b must embed this state and actual retained payload/process/task entries under
one ReceiverState mutex. No independent admission mutex or generic transaction
closure was added. Real registered handles must drive settlement and reaping;
these local transitions do not prove actual WorkerPool or payload completion.
Failed cleanup/handle termination must retain the original owner and charges.
R04c must order its fresh durable view, local claims, replay consume and prepared
owner installation under that same lock, and issue fresh StartCheck receipts
outside it. Initialization must consume R04b's exact registered ProcessReady and
R07's opaque InitializedWorker completion for this retained boot/initialization
identity. No raw disk owner, fake process readiness or listener-writable success
flag is introduced here. Until those owners exist, only fixtures initialize.

Snapshots derive counts from retained records and expose no idle/termination
assertion. No listener, transport, executor, WorkerPool, engine, controller,
heartbeat or final authenticated acceptance integration is delivered. R04,
R03a3, runtime drainage, idle and termination remain unproved. Stop at mini 1.
Cargo/build and Rust implementation ownership is RELEASED to the parent for
specification review followed by quality review.
