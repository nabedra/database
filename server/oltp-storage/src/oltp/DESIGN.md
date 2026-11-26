OLTP storage
============

Every OLTP runtime opens storage through `WorkerRuntime`: typed group stores
inside shared native pools. The standalone RF1 engine and its one-group
OpenRaft adapter were removed with the previous storage engine and their store
formats.

Storage engine
--------------

Each pool holds one `nabedra-kv` store in `<pool>/kv/`. `nabedra-kv` is the
in-tree storage engine in [`server/kv`](../../../kv), written in Rust with no C
dependency: an LSM with a group-committed journal, snapshots, range deletes and
journal values for Raft log bytes. Its file layout, journal format, recovery
and damage rules, and memory charges are specified in the
[native storage engine plan](../../../../plans/native-storage-engine.md). The
pool's glue (shared resources, store options, key scopes, error mapping) is in
[`pool/storage.rs`](pool/storage.rs).

Pool format 2 is the only supported format. A format 1 pool, a directory with
the previous engine's files or a legacy standalone store is refused with
`StorageUnavailable` naming the path, and is left unchanged (NS22). Start such
workers on empty volumes.

Modules
-------

- [`pool`](pool/DESIGN.md): pool identity, worker-shared resources, group
  namespaces, atomic group transitions, snapshot export and install.
- `mvcc`: catalog, MVCC stores, indexes and the query planner on group stores.
- `backup`: backup history, lifecycle and restore (`oltp-backup-s3`).
- `codec`: order-preserving primary-key encoding and prefix bounds.
- `query`: working-memory charges and bounded serialization for the planner.
- `native`: explicit thread and stack reservations (`NativeThreadBudget`).

Tables with search columns
--------------------------

[Vector search](../../../../plans/oltp-vector.md) adds records only to groups
whose table has search columns. Every other group keeps its bytes.

| Record | Holds |
| --- | --- |
| `'x' ‖ row key ‖ column ‖ commit_ts` | One vector: kind, CRC32C and bytes |
| `blobs` and `source` in a `'v'` record | The vector versions the row version references, and the source version of each embedded column |
| `'e' ‖ row key ‖ column` | The pending or failure record of an embedding |
| `'y' ‖ txn ‖ ordinal` | Vectors a prepared transaction staged, until Resolve |
| `0x40 ‖ group ‖ 'c' ‖ log index` | What the entry changed, for the search feed |

Change records belong to one replica, like its log. The log's purge deletes
them, no snapshot carries them, and each names the one before it, so a reader
sees every gap. Readers serve only vectors of the range's search columns
that have their column's type, and a split import or a snapshot install refuses
one that does not. A Resolve at or below the closed floor fails its apply and
quarantines the group: it cannot be refused without breaking atomicity, and the
coordinator's timestamp order should make it impossible (defense in depth). A
damaged vector quarantines its group durably: a synced group marker and pool
record survive a restart, so the replica never opens, votes or serves again
until its release. A restore checks every vector against the backup's search
columns (`HistoryFrame::GroupSearch`) and refuses a mismatch as a schema error.
There is no MVCC version collection. A collector must use
`store::row::unreferenced` for vectors.

Ambiguous writes
----------------

A commit that fails after its journal write may or may not be durable. Any such
error returns `outcome_unknown` and latches the pool into recovery-required
state; every handle of the pool shares the latch, and an absent live value
never authorizes recomputation. Only reopening the store, which replays the
journal, establishes authoritative state again. `NoSpace` is the exception: it
wrote nothing and is reported as a plain failure (O29).

The distributed argument these stores support is in
[OLTP serialization](../../../../docs/architecture/oltp-serialization.md).
