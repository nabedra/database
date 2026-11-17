The local OLTP engine is an experimental standalone RF1 implementation. It is
one RocksDB pool with one primary index per table. It has no distributed failure
tolerance, secondary index, shard split, transaction timestamp service, backup,
or provisioning adapter. An additional experimental one-group OpenRaft adapter
persists ordered state and supports file snapshots. It does not yet provide
per-shard MVCC, cross-group transactions or distributed request retention.
Opening the store requires an explicit engine, format
and topology manifest. A future replicated format must not adopt it silently.

The separate [typed pool foundation](pool/DESIGN.md) now supplies shared worker
resources, group namespaces and group-local snapshots for the next format. It
does not turn this legacy standalone/one-group execution path into the target
MVCC or distributed runtime.

Serialization and recovery argument
===================================

All thirteen operations retain the existing protocol types and validation.
Mutation execution holds the store's writer mutex from conflict checks through
the synchronous RocksDB batch write. This orders uniqueness checks, absence
checks, updates, create/drop and no-op results. A reader creates one RocksDB
snapshot and uses it for schema, primary index and every row in its operation.
It therefore observes the entire before or after state of each atomic batch.
The linearization point of an ordinary read is snapshot acquisition. Successful
mutations linearize at their atomic batch application. Their responses follow
WAL sync. A read already pinned before a mutation completes may return the older
state, which is a valid overlap order. A subsequently started read sees the
completed batch. No global table counter or whole-table buffer participates.

An insert batch checks every submitted key against that snapshot and a bounded
in-batch uniqueness set. Only one WriteBatch becomes visible; a duplicate at any
position discards the complete candidate. Updates preserve untouched columns,
and validation forbids changing the primary key. Create publishes name and
immutable ID together. Drop deletes both descriptor indexes and the table's row
range together; old snapshots retain their previous descriptor and row state.
An old handle cannot resolve to a newly created table with the same name.

For identified mutations, the exact raw-body digest, original run and exact
success or deterministic error are written in the same batch as changed rows.
Repeated identities with a matching digest return the original result; another
digest conflicts. Deterministic conflicts, missing rows, empty batches and
no-ops are stable. The API binds a directory to one persisted database selector
and authorizes every receipt before lookup. Decisions are indexed by UUID inside
that database's store and survive bearer rotation. Identity history currently
has no pruning policy and therefore consumes disk until a future safe retention
protocol is implemented.

An uncertain RocksDB write returns outcome_unknown and latches the store into
recovery-required state. A later lookup can prove a present atomic decision
committed. A missing live value cannot authorize recomputation, because the
native synchronous WAL write precedes memtable insertion and a later error can
leave durable state invisible until recovery. All missing-decision lookups and
new execution then fail closed. Only dropping every live DB handle and reopening
through successful RocksDB recovery establishes authoritative absence again.
There is no API to clear this latch on a live store. Startup never reads a
pending application request from disk.
It opens committed RocksDB state and returns durable results when authorized.
The separate RAM queue abandons outstanding work when its final copy is lost.
There are no persisted user closures, PREPARE records or requests to replay.

The mutex is not a distributed lock. The additional state-machine adapter
applies a deterministic ordered mutation with its applied-log position, durable
result and membership in the same synchronous batch. The leader must resolve a
create operation's UUID before proposal. Local resource/storage failures stop
application rather than becoming different business results on replicas.

Replicated format 2 records the database selector as immutable enrollment
identity. Opening requires the same cluster, group and database, and format 1
standalone stores cannot be adopted as replicas. Replica bind_database only
compares identity and performs no write. Ordinary execute and the public local
snapshot-install helper reject replicas. StateMachine::read requires a fresh
ReadIndex barrier before opening its local snapshot.

Consensus-ordered Fence commands conditionally advance an active run and owner
term. Operation commands check that pair atomically with data, result and applied
position. A committed identity ordered before a new fence remains reconcilable;
an uncommitted old command ordered after the fence cannot change rows. This is a
one-group state transition, not the P5 status/data-shard transaction protocol.

Snapshot records are streamed into an inactive generation in bounded batches.
The full checksum, header/body applied and membership agreement, database/group
identity, and no-backward position check must succeed before the active pointer
and persistent snapshot reference change together. The no-backward check holds
the same writer lock as activation. Consensus keys are outside this range.
Existing RocksDB snapshots retain old generations after range retirement.

The official OpenRaft storage suite exercises the log/state implementations;
it does not prove the deployed network, quorum, timestamp, cross-pool commit,
three-AZ durability, or the P4/P5 failure model. Multi-group pools and bounded
cleanup of superseded snapshot files remain required before those paths ship.

Execution and resource bounds
=============================

Primary get uses a RocksDB indexed get. Equality predicates use that same get
with residual checks. AND-connected key intervals become bounded range seeks;
OR and NOT retain a full residual scan. Scans disable block-cache filling and
decode one row at a time. Count keeps one row and a checked u64 accumulator.
Collect in primary order stops at its limit. Other ordering retains a heap of at
most min(limit, 10001) rows, with ascending primary-key ties even for descending
sort values. No query limit changes the logical table's storage capacity.

Task-owned buffers have conservative charges and bounded serialization. Request,
batch, result and per-task memory bounds are independent of table size. The
executor semaphore is retained inside blocking work, including after a caller
drops its future. Panic containment releases the semaphore.
Raw requests acquire async writer ownership and native execution capacity before
the runtime reserves scheduler CPU. Only then does native work decode the body,
verify its mutation classification/digest and prepare a write. Waiting writers
therefore keep their bounded raw bytes without occupying compute slots. Dropping
an unstarted preparation releases its writer/native permits, and cancellation
of a writer wait decrements its observation counter.
The runtime moves its CPU/memory reservation into execute_with_guard. Native
work returns that guard with ordinary results for encoding and settlement. If
the waiting future is cancelled, the blocking closure retains the guard until
its owned work ends.
Panic recovery clears poisoning only on the writer's unit mutex. That mutex
contains no mutable logical state; private batches drop during unwinding and
atomic durable data remains authoritative. Stateful mutexes retain their own
failure policies.

Native allocations are separately reserved by runtime admission. Cache, write buffer manager and
background-job settings are explicit, but these are not a hard process RSS or
OS-thread limit. RocksDB allocator overhead, pinned blocks, its shared native
environment, Tokio threads and page cache need measured aggregate evidence.
The standalone CLI uses its tested network runtime with 256 blocking slots.
A seven-thread deterministic barrier checks that this configuration does not
retain the old four-thread ceiling. It is dispatch-capacity evidence, not CPU
saturation evidence. Arbitrary library embedding still depends on the caller's
Tokio blocking capacity; shared pools require an owned fixed native executor
before they can advertise independent execution capacity.
Opening another pool currently creates another resource budget, so multi-pool
deployment is unsupported until there is a shared pool registry and allocator.

Dependency spike and evidence scope
==================================

The reviewed Rust binding is rocksdb 0.25.0 with defaults disabled and only
bindgen-runtime enabled. Its published Cargo manifest resolves librocksdb-sys
0.19.0+11.8.1. The upstream master manifest's native 0.20.0 is not the published
dependency and is not evidence for this build. Local tooling was Rust 1.99.1 and
Apple clang 21.0.0. Cargo.lock records the exact selected sources. Native build
and executed test observations belong in the parent evidence ledger.

Primary sources consulted:

* https://docs.rs/rocksdb/0.25.0/rocksdb/
* https://github.com/rust-rocksdb/rust-rocksdb
* The downloaded rocksdb-0.25.0 and librocksdb-sys-0.19.0+11.8.1 sources.

The native ordering was checked in the pinned 11.8.1
db/db_impl/db_impl_write.cc. WriteGroupToWAL and ApplyWALToManifest precede
WriteBatchInternal::InsertInto. The O29/O11 model regression supplies an absent observation
from an older snapshot after a real successful synchronous write, then proves
that the uncertainty latch prevents recomputation until reopen. This does not
inject a real error at that native WAL/memtable boundary.

Local tests address subsets of O01-O04, O07/O08/O10/O11 and O21. A SIGKILL test
after the engine's durable acknowledgment checks process-crash recovery without
destructors. This is not a power-loss/fsync fault model or EBS/EKS evidence.
Crossing 100000 rows and 64 MiB in one local table establishes removal of those
old limits, but does not establish safe shard splits, volume growth or O21 in
full. No unexecuted scheduler or provider case is claimed to pass.
