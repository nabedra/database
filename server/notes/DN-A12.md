# DN-A12: R02a captured handler and registered output deposit

R02a executes one bounded captured `JobEnvelope` and deposits its validated output into an already registered payload owner on the execution thread. This is a design handoff, not implementation or evidence of passing tests.

Parent adoption after independent re-check: accepted for implementation as
R02a1 incremental production, R02a2 captured handler/registered deposit, and
R02a3 local composition. Sender correction/review and sequential file/Cargo
ownership remain prerequisites; this adoption establishes no executed behavior.

[Distributed contracts](../../plans/distributed-contracts.md), [A06](DN-A06.md), and [A09](DN-A09.md) govern this slice. A09 supersedes A06's ownership transfer through closures/results. [A01](DN-A01.md) supplies historical job context only; A06/A07/A09 supersede its framing, drain and cleanup sketches. Read completed [R01a](DN-R01a.md), [rows](DN-R01b2b1.md), [prepared](DN-R01b2b2.md), [owner](DN-R01b2b3a.md), and [receive](DN-R01b2b3b.md) contracts. Sender implementation is concurrently owned elsewhere; this note relies on A09 and these completion contracts, not transient owner/stream code. Adopted A11 configuration, with C08 pending, needs no amendment here.

No applicable `AGENTS.md` was found in workspace ancestors or the repository, including server/notes. Planning, architecture and unslop guidance was applied within the requested note convention. Only this new note is owned. No production files, tests, dependencies or configuration are changed.

## Captured execution contract

Use new private `engine/job.rs`, declared privately by `engine.rs`. Keep its interface synchronous and storage-generic, with no `Engine`, coordinator, catalog resolver or pool argument:

```rust
pub(crate) fn run_captured<B: ObjectStore>(
    runtime: &Runtime,
    storage: &Storage<B>,
    job: &JobEnvelope,
    output: &mut LocalOutput<'_>,
) -> Result<ResultEnvelope, JobError>;
```

`LocalOutput<'_>` is a crate-private worker-scoped borrow of the registered cell's operation account and concrete pending output. Its constructor and fields stay private to `payload_owner`; it has no independent disk ownership and cannot outlive the executing borrow. Its finite methods accept validated rows, a prepared producer, or a sanitized error on the worker. It is not a plugin, dispatcher trait or general executable closure interface. `run_captured` never returns raw output handles. The memory envelope is only a completion status until the owner commits delivery.

Validate the constructed job before schema indexing or storage access. Use `encode_job` for bounded constructed CONTROL validation, not a JSON parse roundtrip. The owner binds the exact expected job, operation account and configured cluster/storage context before execution. Reject a different cluster/account operation, job/attempt, digest, snapshot or ordinal at deposit. Copy all result identity fields from that job and run `ResultEnvelope::validate_for` and `encode_result` before success. Never mint a new attempt or silently execute locally after another path fails.

Convert `FileInput` directly to private `partition::ReadFile`: legacy object/count, or an exact `PartitionFile` with object/range/count/encoded size/payload. Reuse `partition::load_file(runtime, storage, table, file)` outside `runtime.block_on`; that loader alone uses the worker runtime for bounded object GET. It already enforces canonical table-object identity, legacy 80 MiB/partition 64 MiB download caps, exact partition encoded size, snapshot decoding/count, complete schema/scalars/unique keys, payload, range and ascending partition keys. Preserve legacy ordering tolerance. No visibility change to this loader is needed inside `engine`.

The trusted coordinator must have captured a validated catalog entry, descriptor/table UUID, generation, file, range and target together. `JobEnvelope::validate` checks their structural consistency, not catalog membership. Snapshot revision/commit/etag are echoed identities; the handler cannot establish their association with an arbitrary descriptor/file by parsing them. Parquet has no independent catalog-generation proof. Do not invent such a check, refresh the catalog, recompute the original request digest from a partition submutation, or treat authentication as proof of complete query execution. Actual-file checks prove schema/content/accounting compatibility, not historical provenance or worker honesty.

For Prepare, require every addressed mutation key to belong to the captured `range`, including every InsertMany row, before `mutation::apply`. Use `KeyRange::contains` and `partition::row_key`, not a new scalar comparator. File/range agreement remains `JobEnvelope::validate`'s rule. `file: None` is the coordinator's trusted empty-input claim; use zero rows without GET, never infer emptiness from a missing object. A legacy input requires the whole range. Validate changed rows against the same range through the encoder too.

| Task | Exact local result and reused implementation |
| --- | --- |
| Get | Load once, record actual source row count/payload before consuming rows, find the typed key with `partition::row_key`. Store zero or one complete row and `found`, including normal absence. |
| Collect | Record source accounting first, then `query::local_collect(table, query, loaded.rows)`. Store every matching full row in its existing comparator order, with ascending primary key ties. Ignore public select/limit even when limit is zero. |
| Count | `query::local_count(query, &loaded.rows)`, with the same actual source accounting. Never call public `query::count`, which applies limit. |
| Prepare | `mutation::apply(table, &mutation.operation(), rows)`. Map `MutationResult::{Null,Count,Found}` exactly to `Effect`. `changed == false` gives null count/`None`; changed with no rows gives zero/`Some(empty)`; otherwise encode the captured range/target. Found true does not imply changed. |

Read counts must equal captured `FileInput::row_count`, with partition payload equal to captured metadata and legacy payload measured from decoded rows. Preserve counts even for absent/empty filtered results. Prepared CONTROL has no source-count fields: verify its input through the loader, do not invent wire fields. Convert `target_bytes` by checked conversion and the existing `PartitionSize` parser using its `B` representation; no public byte constructor is needed.

Retain `mutation::apply`'s current single-candidate growth, scalar equality including float bits, batch atomicity and validation. Do not add a lower per-partition query/result/response budget or apply an operation quota afresh for each file. Global source totals, checked Count sum then one limit, Collect sorting/projection/limit, final 10,000-result/8 MiB envelope, whole-table 100,000 rows/64 MiB, range repair and catalog encoding stay with coordinator finalization. No object PUT, catalog publication, final object naming or result cache occurs here.

## Incremental encoding and legitimate local prepared production

The current `snapshot::encode_partition_range` returns `Vec<EncodedPartition>`, retaining all candidate bytes. Calling it and then spooling the vector does not meet R02a's allocation contract. Extract its existing algorithm into a crate-private fallible emitter:

```rust
pub(crate) fn encode_partition_range_each(
    table: &TableDescriptor,
    rows: Vec<Row>,
    range: KeyRange,
    target: PartitionSize,
    emit: impl FnMut(EncodedPartition) -> Result<(), ProtocolError>,
) -> Result<(), ProtocolError>;
```

Keep `encode_partition_range` as the unchanged public collecting wrapper. Share validation, typed sort, midpoint split stack, left-before-right range emission, singleton soft-target exception, hard-cap bounded sink and footer checks. Drop an oversized attempt before children. Emit one complete encoded file and release its bytes before encoding another. Do not duplicate Parquet/scalar rules or change standalone write behavior. Emitter failure stops immediately and leaves all partial spools private for worker disposal.

`PreparedReceiver::new` requires the exact final replacement count, which is unavailable until splitting completes. Do not guess that count, encode twice, stage a second spool copy, or expose an unchecked `ValidatedPart` constructor. Add one crate-private `LocalPrepared` producer inside `payload/prepared.rs`:

```rust
impl LocalPrepared {
    pub(crate) fn new(operation: Arc<SpoolOperation>, job: &JobEnvelope,
        effect: Effect, changed: bool) -> Result<Self, JobError>;
    pub(crate) fn push(&mut self, part: EncodedPartition) -> Result<(), JobError>;
    pub(crate) fn finish(self) -> Result<PreparedResult, JobError>;
}
```

All three calls, rejection paths and producer destruction execute on workers. Map a producer error to a fixed `ProtocolError` only at the fallible encoder callback; preserve its code without copying private error text.

- Start from the validated expected job, shared operation account and exact mutation effect/change flag. Unchanged output accepts no files; changed-empty accepts an empty sequence.
- Consume each emitted `EncodedPartition` on the worker. Derive length and digest from its actual bytes with shared SHA256 formatting/update logic from the existing receiver. Validate metadata before opening a capped writer, append at most 64 KiB per call, close it, then invoke the same actual-file verification used by `PreparedReceiver::end_part`.
- Extract only the common closed-file verification and digest helpers within that private module. Verify actual length/hash, `snapshot::decode(..., Some(row_count))`, and `engine::validate_partition_rows` for exact payload, key range and strict key order. The locally supplied metadata receives no trust exemption. Drop the decoded rows/Bytes before processing the next file; no claim of streaming Parquet decode is made.
- Retain only opaque validated parts and bounded metadata. Enforce checked running counts/rows/payload/encoded bytes and the existing 110,000 preparation rows/files bound before retaining further output. At completion, construct exact nullable `replacement_count`, validate the result, and run the existing `PreparationValidator::new/push/finish` over those parts, including null/zero. Range gaps, coverage errors or any partial failure prevent `PreparedResult` construction. Keep any shared counter extraction small and private, without changing wire records or validator behavior.
- Only this module constructs `ValidatedPart`/`PreparedResult`. Export `LocalPrepared` crate-privately through `payload.rs`; keep existing public receiver/read APIs intact. Local completion returns `PreparedResult` only synchronously into the borrowed registered output cell on the same worker.

Expose the existing row semantic `validate` helper crate-privately through `payload.rs` for direct typed Get/Collect acceptance. Both local and remote paths use it, including Get key/source checks and Collect uniqueness, predicate/order and declared-count checks. The validator cannot prove that a peer omitted no matching rows; the shared handler establishes completeness by processing the entire captured input. Local rows need no JSON spool/decode roundtrip. Remote sending subsequently uses existing `encode_rows`; remote receive uses existing `decode_rows`. Count/Error retain only validated memory controls. Sanitize failures to bounded fixed messages with existing error codes; job failures never assert a database outcome or expose OS/parser/path text.

The working set is bounded source/candidate rows plus one encoded attempt, codec working memory, one-file validation Bytes/rows, bounded metadata and one chunk. These can overlap and need process concurrency accounting; 64 MiB logical payload is not a claim about resident allocation size. One process Spool and one shared operation account cover all files, attempts and retained delivery. Existing writer accounting checks process and operation byte/file quotas before writes. Exhaustion fails without publication; no giant reservation, aggregate 80 MiB output limit, per-job account or unbounded test scratch owner is permitted. Reader concurrency remains independently bounded, normally one active file reader per command.

## Registered worker-side handoff

Extend the reviewed owner only after the concurrent sender owner releases its files. Use one private local-production admission entry, generic only over the existing storage backend. Its inputs are the operation lease, role/deadline/lifetime guards, exact boxed job, `Arc<Storage<B>>`, injected partition pool and already admitted partition execution/queue responsibility. Its output is the ordinary opaque noncloneable payload lease. Refusal and pending inputs own memory only.

Registration must install an EMPTY disposal cell and its guards under the existing admission/close lock before spawning the existing supervisor or submitting any partition work. Process/operation anchors are already registered and initialized by A09's worker commands. Borrow the account from those cells only after execution starts. Do not capture a cloned raw `Arc<SpoolOperation>`, Spool, PreparedResult, ValidatedPart or reader in a pending/rejected closure.

The existing registered supervisor gets one local-production prelude: submit one partition closure, retain its submission future, await accepted `Job` to settlement, then enter the existing lease/delivery loop. The closure captures registered cell references and memory inputs only. Inside execution, borrow the local output cell/account and call `run_captured`. Install prepared/row output there before returning a memory-only status/envelope. Catch unwind inside this cell access, dispose worker-local temporaries there and mark failed. No raw output crosses the `Job<T>` oneshot, including buffered success or a failed send. Do not add another task registry, generic command runner, fallback executor or nested pool submission.

Serialize ownership explicitly: while Producing, no CPU/I/O command can read or discard that cell. Cancellation sets sticky memory state; the supervisor first settles the partition job, then submits Discard to the separate bounded coordinator CPU/I/O pool. A worker may also destroy a failed unpublished candidate before returning. Preserve the existing short handoff lock and cancellation ordering. Add a distinct typed-local completion transition after full semantic validation; never fabricate transport EOF or call a framed CommitEof without EOF. Copy accepted memory status plus the lease to the caller, and recheck cancellation before delivery.

Check sticky cancellation before optional source work and between emitted files; an executing synchronous load/decode/encode may finish before cancellation is observed. This is cooperative cancellation, with no promise to interrupt an OS call or reclaim an accepted job early.

Partition execution capacity can end after computation and deposit. The owner retains delivery admission, operation/dependency lifetime, spool quotas and cleanup responsibility through sending or local consumption, explicit worker Discard, actual supervisor termination and registry reaping. Rejected submit creates no disk owner; cancellation/panic/buffered ticket drop signal this same retained owner. A missing reply or unavailable cleanup pool retains bounded failed-drain state and cannot report idle. Async code never extracts disk state or blocks on its disk mutex. No destructor starts cleanup work. Preserve stable broadcast reaping and early-error disposal corrections documented in the completed notes.

This prelude supplies only one admitted job's supervised handoff. Admission policy, remote placement, auth, A07 dependency records, runtime startup/shutdown wiring and replacing public Engine closures remain separate assignments. Handler code acquires no operation permit and invokes no pool. The runtime keeps the distinct CPU/I/O owner pool alive through output and anchor disposal before partition/owner lifecycle teardown. Local typed completion and remote framing have identical logical validators/results; only a real framed receiver requires STREAM_END plus actual EOF.

## Dependency-ordered mini assignments

Each mini needs its own bounded implementation/review handoff. Parent serializes shared files and Cargo ownership; no delegation or commands are requested by this design.

1. **Incremental production.** Own `snapshot/partition.rs`, its private tests in that module, the crate-private re-export in `snapshot.rs`, `node/payload/prepared.rs`, its private tests, `node/payload/rows.rs` helper visibility and narrow `node/payload.rs` exports. Deliver the emitter/collecting wrapper and checked `LocalPrepared` producer described above. Compare existing wrapper versus emitted files/ranges on a tiny forced split; verify real spool-produced parts and null/empty completion. One corrupted local metadata/file case must fail through the shared actual-file verifier. Reuse codec/scalar tests; no new public constructor or integration-test facade.

2. **Captured handler and registered deposit.** After mini 1 and reviewed sender completion, own new `engine/job.rs`, private `engine/job/tests.rs`, narrow `engine.rs` declarations, local-production additions in `node/payload_owner.rs`, private `payload_owner/tests.rs`, and necessary crate-private node re-exports in `node/mod.rs`. Implement `run_captured` together with registered-empty admission, supervised partition prelude, worker-only `LocalOutput` access and typed-local completion, so neither side needs a placeholder implementation. Existing read/write/partition/query/mutation bodies, runtime, storage, frame and spool remain unowned. Own `node/job.rs` only to make the existing `RowMutation::operation` method `pub(crate)`, preserving its body and all validators. Reuse `tests/support/storage.rs`'s actual Fake through test-only inclusion/aliasing without a public test facade; seed small real Parquet objects directly and snapshot `puts`/`put_keys` after seeding. Assert no additional object writes in successful and failing cases.

   Use the real small owner/pool/spool fixtures from the completed owner tests. Gate one accepted producer before deposit, cancel its caller, and prove admission/account lifetime survives through job settlement and worker disposal/reaping. Cover pre-acceptance rejection, panic after creating one candidate and a dropped buffered completion ticket. Extend existing causal hooks only for this prelude; reuse generic stream/WorkerPool failure tests. No test-only alternate owner or executor. Keep setup/teardown disk anchors in the production owner discipline.

   A few grouped checks cover captured cluster/table-object/range rejection and a mismatched deposit attempt/snapshot; wrong captured row/payload/encoded-size input; absent Get; Collect with select/limit yielding all full locally ordered rows; unlimited Count even with limit zero; Prepare unchanged found-true, changed-empty and forced split. Verify exact effects/counts and captured generation use with a conflicting newer catalog present. Out-of-range InsertMany must fail before applying a successful prefix. Use current scalar/range APIs rather than repeating their matrices.

3. **Small composition fixture.** After mini 2, own only a private test addition in `node/payload_stream/tests.rs` after its owner releases it. One tiny changed/split Prepare goes handler → registered sender → existing framed receiver over Tokio duplex. Compare exact envelope and verified replacement metadata/content through private worker commands, explicitly finish and shut down the writer to supply EOF, then dispose both output leases, anchors and worker lifecycles. Assert all owned tasks settle and scratch/quota is released after normal disposal. Reuse the existing receiver's explicit-EOF rejection tests; this composition proves wiring and output disposal, not another full framing matrix. Direct private API tests stay in their owning modules.

The minis must reuse already passing row/scalar/codec/WorkerPool/receive/send evidence. No full duplicate matrix, retries as evidence, network/process claim or distributed progress/drain guarantee follows from these local fixtures. Any future test timeout is a failure watchdog, not proof of cancellation or cleanup.

## Parent decisions and completion

No new protocol, schema, dependency, target semantics or public configuration decision is required. Parent must accept this private emitter/local-producer correction and serialize payload owner/stream edits after the concurrent sender review; exact entry-point spelling can follow that reviewed implementation. Runtime capacity values, authentic captured-context establishment, global finalization/uploads and remote Engine dispatch remain existing parent work, not missing handler decisions. No further semantic decision is left open here.

This note was produced by read-only source inspection. No implementation, tests, builds, Cargo/suite/probe commands, secret/.env reads, cloud calls, delegation, branches/worktrees, commits or pushes were performed.

Independent design review found one implementation ownership gap: the handler
must call the existing private RowMutation::operation helper. The parent added
only its crate-private visibility change to mini 2's allowed files, preserving
the method body and validators. Fresh design re-check passed without essential
remaining corrections, and the parent adopted the design in the shared contract.
