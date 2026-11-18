This is the typed multi-group storage foundation. It is separate from the
standalone RF1 and experimental one-group formats described in
[the earlier design](../DESIGN.md). It is not the completed per-shard MVCC,
distributed queue, index, placement, or transaction engine. The conditional
[distributed serialization argument](../../../../docs/architecture/oltp-serialization.md)
still requires those implementations.

Pool identity and resource ownership
====================================

An explicitly selected development directory has a physical pool-format-1
manifest containing engine, cluster, worker and pool UUIDs. An existing legacy
directory, foreign engine, changed identity or unidentified nonempty database
is rejected. Typed group-format-1 is a different namespace/version from both
physical pool-format-1 and the earlier one-group replica-format-2.

The registered-volume constructor accepts only a sealed verified-mount type
and currently returns UnsupportedOperation. There is no production fallback
that creates a directory on the root disk when a mount is missing. Provider
verification, anchored directory handles, capacity reservation, enrollment and
cross-worker ownership remain P7 work. Development directories establish none
of those provider claims.

Every volume on a worker receives the same WorkerSharedResources. It owns one
ResourceScheduler, cache, write-buffer manager and actual fixed query/control
executors. Cache/write-buffer limits do not multiply when another pool opens.
Each open pool adds a separately charged native-overhead reservation. A live
resource owner binds one worker UUID, and a process registry rejects a second
owner for that UUID. Wire buffers, decoded payloads, prepared native work and
the physical queue lease retain that owner until their last reference drops.
A surviving payload therefore cannot reset its worker's ceilings by dropping
the original pool or transport handle.

The physical queue claim permits one composition owner. That owner must contain
all database scopes under shared count, body and result ceilings. It does not
establish multi-database queue composition by itself. Storage handles use a
bounded weak registry, with historical metadata paged on disk. Strong Raft task
residency is a separate bounded lease; retiring an advertised voter still needs
the committed membership and joined-task lifecycle implemented by its owner.

Native execution and memory
===========================

The default shared profile has one query executor, one control executor and two
native maintenance execution threads. This fits max_threads=4. RocksDB requires
at least one HIGH and one LOW thread: pinned db_impl_open.cc sanitization grows
both pools, and GetBGJobLimits clamps each to at least one. Every open explicitly
sets one flush job, background-1 compaction jobs and one subcompaction, so larger
budgets cannot silently grow the HIGH pool beyond one. The process-shared Env
rejects another incompatible background configuration.

RocksDB also has one shared periodic scheduler timer. PeriodicTaskScheduler::Default
owns that timer, and DBImpl::StartPeriodicTaskScheduler registers log-flush and
compaction-trigger tasks on it. It is reported separately from maintenance
execution capacity. Tokio/network runtime threads and the main thread are also
separate overhead; total process thread count is not query+control+background.

Rust executor threads explicitly request 2 MiB stacks and reserve 4 MiB each.
Native threads reserve at least 8 MiB each, enlarged to the observed pthread
default stack plus guard plus 64 KiB. The shared timer receives that same native
stack reservation. Arithmetic includes every allowed background thread,
including the 128-thread configuration. These are conservative logical
stack/guard/metadata reservations, not peak RSS or committed physical RAM.
Changing process pthread defaults outside the worker after initialization is
outside this resource profile. Native stack accounting is currently verified
on Unix; unsupported platforms fail explicitly.

Wire Data and Progress have separate fixed byte arenas. Progress is selected
from the authenticated endpoint role, not a sender-provided class. Data decode
scratch instead receives a dynamic MemoryCharge from the same scheduler, with
an actual lexical allocation bound before deserialization. Progress decode
retains its protected arena. Body copies and terminal results must transfer to
the queue/result owner instead of retaining a transport arena indefinitely.

Native scratch and WriteBatch capacity come from separate precharged arenas,
64 MiB per physical slot by default. Data/Catalog work uses query slots;
Control/TSO/status work uses control slots. Transport/query working-memory
saturation cannot consume those native apply reservations. Query and metadata
replies have independent bounded arenas. Reply capacity is acquired without a
physical or CPU permit. Physical capacity precedes native apply buffers and the
CPU grant. An oversized configured job fails before native growth; a temporary
reply-capacity wait retains no CPU or physical slot. Mutation replies use the
metadata lane even when their data transition runs on a query executor.

Normal returned bytes move to reply ownership before the native slot ends.
Decoded log commands use separately owned scheduler scratch. Native buffers
must not become long-lived log entries, queue records or transport payloads.
Cancellation moves the actual guards into native work; an abandoned receiver
does not release them while that work continues. The outer executor boundary
also catches a guard-destructor unwind after receiver cancellation.

RocksDB's cache and write-buffer settings are not a hard RSS limiter. Pinned
blocks, allocator fragmentation, native write-buffer overshoot and the OS page
cache still need aggregate measurements and deployment limits. Per-pool native
overhead and per-job reservations are conservative accounting, not a claim that
every native allocation has been intercepted.

Atomic group transitions
========================

One RocksDB database contains many groups. Each group has an immutable identity
and a writer-order gate shared by all its handles. Application keys are scoped
to group UUID plus active generation. Log keys are scoped to group UUID in a
generation-independent domain. Applied position, membership, response and
snapshot references use internal keys unavailable through application get/put.

A transition waits for group order and reply capacity before physical/CPU
admission. It reads one native snapshot. Get and scan deliberately see that
snapshot only, not candidate WriteBatch changes. Callers load a counter once,
compute its next value locally and write the fixed result. Key/value/batch
charges are checked before batch growth.

Commit writes candidate effects, applied position, optional membership and exact
response in one synchronous batch. Reject discards candidate application writes
but still commits the ordered response and application metadata in one batch.
Replaying the last exact position/digest returns its persisted response without
calling the transition again. Conflicting or nonsequential positions fail.
No independent mutable cache is authoritative. This is a local ordered-storage
argument, not a cross-group transaction proof.

Any ambiguous native write latches recovery for the physical pool. All group
handles share it. An absent live value cannot establish rollback after RocksDB
may have synced WAL before updating its memtable. Only dropping every DB handle
and recovering on reopen establishes authoritative state. The O29 model tests
are not native fsync or power-loss fault injection.

Snapshot activation and cleanup
===============================

An export streams one native snapshot into a file. Its header binds immutable
group identity, applied position, command digest and membership/response hashes.
Records and header have one checksum. The file and directory are synced before
a durable reference can name it.

Installation copies into owned storage, then stages bounded records under an
inactive group generation. Checksum, record ordering, identity and header/body
application metadata must agree. The same writer-order boundary checks stale
positions and activates the generation, applied state, membership, response and
snapshot reference atomically. Raft logs and every other group's keys remain
outside that replacement.

A staging marker survives interruption. Reopening the group removes its inactive
staging ranges and failed files. Activation retires only that group's preceding
generation using range tombstones. Existing RocksDB snapshots retain their old
cut. Superseded file paths are unlinked only in the group's owned directory;
Arc<File> transfer handles pin their bytes on POSIX after unlink. A transfer must
use its open handle instead of reopening a retired pathname. A missing/invalid
current reference fails before garbage collection.

Record buffers are bounded independently of total table size. The current local
export/install loop still occupies one query executor for the stream; incremental
maintenance scheduling and provider crash/space testing remain obligations.

Fixed-command payloads and evidence
===================================

CanonicalJson uses compact JSON with sorted object keys and preserves number
token spelling. Public constructors create charged immutable bytes. A bounded
codec may temporarily deserialize private unattached RawValue storage under its
already reserved decode owner. The outer command sums metadata/raw capacities,
checks the aggregate, and attaches that same owner before escape. Serialization
and application reject unattached values. Clones and extracted fields retain
the same owner rather than copying whole rows.

The internal fixed-command ceiling is 32 MiB, separate from the unchanged 8 MiB
application body. Fixing a primary key separately can enlarge the internal
representation. Wide log reads probe a pinned value's budget/hash, release
native/CPU, reserve actual decode memory, then reopen and verify the value while
decoding into an owned command. A changed uncommitted log range requires retry.
Raw entries need not fit the 8 MiB query reply arena, and their returned owners
do not retain native apply scratch.

The named assertions in pool/tests.rs, native.rs and mvcc.rs address parts of
O05/O10/O11/O16/O29, C01/C07, M01/M02, Q02 and F02/F03. Native thread probes run in
isolated processes. Linux can assert named HIGH/LOW pool counts; macOS observes
total thread deltas with executors/network absent. Snapshot interruption tests
model before/after durable activation and actual reopen, not physical power
loss. Exact executed source hashes, commands and raw logs belong in the parent
evidence ledger. These tests do not prove per-shard consensus, MVCC intent
discovery, online indexes, distributed commit, queue takeover, EBS durability or
autoscaling.
