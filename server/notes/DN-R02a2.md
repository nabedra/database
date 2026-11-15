# DN-R02a2: captured handler and registered local production

Implemented adopted A12 mini 2 on shared main. Applied the requested
test-driven-development and unslop guidance after reading the distributed
contracts, A12 adopted handoff, A06/A09, completed R01 payload/owner/receive/send
and R02a1 notes, and the actual partition, query, mutation, job, payload,
owner, stream, WorkerPool and Fake interfaces. No applicable AGENTS.md was
found in workspace ancestors or owned directories.

Existing uncommitted work was preserved. No delegation, branch, worktree,
commit, push, secret or .env read, cloud call, dependency fetch, target deletion,
configuration change or startup-material edit occurred. Public engine and
standalone behavior are unchanged. C08b1 remained outside this assignment.

## Files and interfaces

Owned implementation is new engine/job.rs; private engine/job/tests.rs;
engine.rs's private declaration and crate-private handler export;
node/payload_owner.rs and its private tests.rs/local_tests.rs; node/mod.rs's
crate-private LocalOutput export; node/job.rs's existing operation method
visibility; removal of R02a1's obsolete allowances in payload.rs/prepared.rs;
and this note. No stream test or implementation was edited. Handler tests
are included privately from the owner's local_tests module so they reuse its
registered production fixtures without a public test facade.

```rust
engine::run_captured<B: ObjectStore>(
    runtime: &Runtime, storage: &Storage<B>, job: &JobEnvelope,
    output: &mut LocalOutput<'_>,
) -> Result<ResultEnvelope, JobError>;

PayloadOwners::try_bound_operation(
    &self, process: &PayloadLease, limits: SpoolLimits,
    lifetime: Arc<OwnedSemaphorePermit>, operation_id: Uuid,
) -> Result<PayloadLease, OwnerError>;

PayloadOwners::try_local<B: ObjectStore + 'static>(
    &self, operation: &PayloadLease, role: Role,
    lifetime: Arc<OwnedSemaphorePermit>, deadline: Option<Instant>,
    cluster_id: Uuid, job: Box<JobEnvelope>, storage: Arc<Storage<B>>,
    partition: WorkerPool, execution: OwnedSemaphorePermit,
    queue: OwnedSemaphorePermit,
) -> Result<PayloadLease, OwnerError>;

PayloadLease::local_complete(self)
    -> impl Future<Output = Result<(Box<ResultEnvelope>, PayloadLease), OwnerError>>;
```

The existing generic framing operation constructor remains available for its
untyped fixtures. Local production requires the explicit UUID bound at
operation creation. Configured cluster identity and the injected storage handle
are independent trusted inputs, not inferred eligibility from the job. The
exact admitted job remains the expected identity during production/deposit.
LocalOutput's fields and construction stay private to the owner. It borrows the
registered disk cell only on a worker and owns no independent spool account.

The handler validates encode_job before indexing or GET, converts FileInput
directly to ReadFile and reuses load_file. It records actual source accounting,
returns complete Get rows, retains all full Collect matches in comparator/key
tie order and counts without the public limit. Prepare checks every addressed
key before mutation::apply, including the entire InsertMany. It preserves
Found(true) without changes, unchanged None, and changed-empty Some(0).
The fallible incremental encoder feeds LocalPrepared using the same operation
account. Any encoder failure abandons the producer. There is no vector of all
encoded files, new catalog read, PUT, publication, cache, projection, public
limit, global finalization, operation-permit acquisition or nested pool work.
External diagnostics are replaced by fixed messages. Result identity is copied
from the captured job; encode_result invokes validate_for before typed deposit.
Rows use the existing semantic validator. Complete-input processing belongs to
the handler rather than that validator.

## Lifecycle and caller obligations

The existing registration path installs an empty cell, guards and registered
JoinHandle under its admission/close lock. A one-shot startup gate prevents the
same supervisor from running before registration finishes. The one typed local
prelude is a private boxed future to erase only the existing storage backend;
commands remain the existing closed enum. No executor registry, generic
executable command API, fallback pool or detached cleanup was added.

The prelude retains its submission future and accepted partition Job through
settlement. Its closure captures memory and registered cells, obtains the
operation account after worker execution starts, and deposits output before a
memory-only reply. The producing flag rejects coordinator commands during this
phase. Unwind is caught inside cell access so candidate temporaries drop on the
partition worker. Only after settlement can the supervisor submit Discard to
the separate coordinator pool. Queue ownership ends on acceptance; execution
ownership ends after computation/deposit or failure. Transport/dependency
lifetime, scratch and anchor retention continue through disposal and actual
supervisor reaping. Stable broadcast reaping, parent holds and sender reader
closure ordering are preserved.

Call local_complete for typed completion. It rechecks sticky cancellation and
settles disposal/reaping before early errors return, or propagates FailedDrain
with the registered resources retained. Dropping a lease or its completion
future signals cancellation only. Buffered replies contain no disk ownership.
No async code locks the disk cell or finally drops disk-backed output. This
transition does not fabricate framed EOF.

The coordinator must capture descriptor, table/object/range and catalog
generation together. Structural validation and actual Parquet checks cannot
prove historical membership, generation, etag provenance or peer honesty.
Prepare file None is a trusted empty capture, never a missing-object fallback.
Runtime routing/auth/controller/shutdown integration and R02a3's handler to
framed receiver roundtrip remain later work. Runtime must retain the separate
coordinator pool through output and anchor drain, supply genuine configured
context and admitted execution/queue guards, and perform global finalization
before publication. No distributed, network, peak-RSS or interruptible OS-call
guarantee follows from these local tests.

## Verification

Every Cargo command used the existing target and this environment:

```text
CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
```

All commands used --manifest-path server/Cargo.toml --locked --offline
--no-default-features. The first focused test preceded production changes.
After correcting a test setup call, its compile red was missing
try_bound_operation and try_local. It was a missing-API red, not an assertion
failure. Subsequent development corrected strict Query fixture fields and
private test helper paths/visibility. The first broader regression exposed a
readiness cancellation check that also affected an existing expired-deadline
anchor. Restricting that check to the local prelude restored existing behavior.
An attempted future-runtime type re-export exposed the private ReceiveKind;
it was removed, leaving only the necessary LocalOutput export and no new
allowance. No suppression was added to hide either issue.

Final commands, with the shared flags and environment above:

```text
cargo test ... --lib node::payload
30 passed, 0 failed, 52 filtered out; exit 0, no warnings, 5.59s.
This includes all 19 prior owner/receive/send groups, six new groups and five
existing private row/prepared groups.

cargo test ... --lib node::payload_owner::local_tests
After the final comparator/range/quota checks: six passed, 0 failed,
76 filtered out; exit 0, no warnings, 0.05s.

cargo clippy ... --lib -- -D warnings
Exit 0, no warnings, 8.48s.
```

The six new grouped tests use the existing actual Fake GET and small real
Parquet, assert unchanged puts/put_keys after seeding on successful and failing
paths, and cover configured cluster/account mismatch, malformed table/object/
range/schema before I/O, mismatched deposit job/attempt/snapshot, wrong source
rows/payload/encoded size, missing objects, captured snapshot despite a different
newer catalog, absent Get, full locally ordered Collect with select/limit zero,
unlimited Count, unchanged Found(true), changed-empty, forced splits, a late
out-of-range InsertMany key, and encoder failure after partial private output.
Scalar/codec matrices are reused.

Lifecycle checks use actual registered anchors, separate partition/coordinator
pools, a candidate gate before deposit and a supervisor gate before return.
They prove scratch quota remains charged while the producer is held and is
released after worker cleanup; execution capacity ends after settlement while
lifetime/admission survive through reaping. Worker thread IDs distinguish
partition execution from coordinator disposal and the async caller. Cases
include pre-acceptance pool rejection, candidate panic and a causally buffered
completion drop. Successful paths release gates, dispose anchors and join both
pool lifecycles. Timeouts are watchdogs, not timing evidence. No alternate test
owner/executor or framing shortcut is used.

No existing engine check was needed because public engine/read/write/partition/
query/mutation behavior was not changed. No broad/default-AWS suite was run.
Owned rustfmt --check, scoped git diff --check and direct whitespace/final
newline checks cover all eleven owned files, including untracked files.
Cargo/build ownership is released on delivery. Independent specification and
quality review remain parent-owned.

## Parent correction after quality review

The quality review found an unbounded coordinator-command observation while
the producer held its gated disk mutex. If the Producing exclusion regressed,
that observation could prevent the test from reaching either gate release.
The parent changed only `node/payload_owner/local_tests.rs` and the single
shared quota assertion in `engine/job/tests.rs`; production code is unchanged.

The gated scenario now captures fallible observations under watchdogs, releases
both gates on every observation result, drops the original lease, reaps the
registered supervisor, and completes fixture teardown before asserting the
captured results. It retains observations of quota, execution/lifetime permits
and worker identities at the original causal boundaries. The quota helper
returns failure instead of panicking in the async observation. Fixture finish
attempts both anchor disposals, registry drainage, both pool joins and directory
removal before its result/counter assertions.

The first check caught one shared caller still expecting a bool from the changed
quota helper; updating that assertion resolved this compile error. This is not
claimed as behavioral red. To exercise the reviewed hang path without editing
production, the parent temporarily set the gated slot's Producing flag false
immediately before the coordinator-command probe. The corrected test failed
with `coordinator command waited on gated production` after 10.07s (exit 101),
at the assertion after successful fixture teardown. Thus the worker-blocking
regression reached both releases, reaping, pool joins and scratch removal rather
than hanging. That temporary injection was then removed completely. The old
hanging test was not deliberately run indefinitely, and no production fix is
claimed by this harness check.

Using the same resource environment and locked/offline/no-default flags above:

```text
cargo test ... --lib node::payload_owner::local_tests::local_candidate_cancel_panic_and_buffered_drop_reap_on_workers -- --exact
Temporary regression injection: 1 failed as intended, 10.07s, exit 101.

cargo test ... --lib node::payload_owner::local_tests
Injection removed: 6 passed, 0 failed, 0.05s, exit 0.

cargo clippy ... --lib -- -D warnings
Passed, 9.17s, exit 0.

rustfmt --edition 2024 --config skip_children=true --check server/src/node/payload_owner/local_tests.rs server/src/engine/job/tests.rs
Passed, exit 0.
```

Scoped diff and direct untracked-file whitespace/newline checks passed. The
earlier thirty-group result is reused, not represented as a new broad rerun.
Fresh independent quality re-check follows. Cargo/build ownership is released.
