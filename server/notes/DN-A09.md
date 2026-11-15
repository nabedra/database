# DN-A09: supervised framed payload assignments

Design only, accepted by the parent on 2026-11-11 after independent re-check.
This reconciles accepted [A06](DN-A06.md) with the
implemented synchronous APIs. [Distributed contracts](../../plans/distributed-contracts.md)
and [A07](DN-A07.md) remain authoritative. It splits R01b2b3 into three sequential
minis. R02a remains the later typed local handler.

Read the contracts, A06, A07, reviewed R01b1/R01b2a/R01b2b1/R01b2b2 notes,
worker_pool.rs, node/frame.rs, spool.rs, payload.rs, payload/rows.rs,
payload/prepared.rs, relevant job.rs validators and existing focused tests.
No applicable AGENTS.md was found in workspace ancestors or server/plans.
Applied the unslop skill. Only this new note was written. No build, Cargo,
test, cloud, .env, dependency, branch, commit or delegation work occurred.
All conclusions below are source reasoning and proposed acceptance checks,
not newly executed tests or network/cluster evidence.

## Selected boundary

Use one concrete synchronous PayloadProcessor and one private async supervisor
per admitted stream. Inject the separate bounded coordinator CPU/I/O WorkerPool,
transport/validation admission, deadlines and the existing operation spool
account. Never use the partition pool. The supervisor owns FrameReader or
FrameWriter; the processor owns all disk-backed state. No pool command waits
for network, peer jobs or another pool command. Perform synchronous disk,
Serde, Parquet and final resource destruction outside runtime.block_on.

A06's instruction to transfer processor state to a command needs a precise
ownership rule with the actual submit API. Do not put exclusive disk ownership
inside the submitted closure or its returned T. Keep that ownership in a
bounded registered slot. Commands borrow the slot only after execution starts
on a pool thread. They return memory-only replies. This is a payload-specific
owner, not a scheduler, public class or generic closure/protocol-trait layer.

Keep logical AcceptedResult and SendableResult private. Their Get/Collect,
Count, Prepared and Error variants retain A06's meanings and exact envelopes.
Prepared's Option<Vec<ValidatedPart>> lives inside the registered processor.
An async-facing accepted result exposes its envelope and logical kind through
memory-only data and holds an opaque, noncloneable payload lease. It never
exposes a raw PreparedResult, ValidatedPart, SpoolPayload or SpoolReader to an
async caller. This representation preserves unchanged None versus changed
Some(empty) while placing destruction correctly. Rows may be consumed into
ordinary owned Row values after acceptance because they own no disk state.

## Actual APIs and hazards

- WorkerPool::submit owns F while waiting for its selected worker's queue.
  Each worker accepts one executing and one queued job. An arbitrary number of
  callers can otherwise wait in submit. Stop can reject F before acceptance,
  including after reservation. A closure capturing a writer or the last spool
  reference would then destroy it on the submitting Tokio task.
- Successful submit returns Job<T>. Dropping Job or its receive future never
  cancels accepted work. The worker sends Result<T, ProtocolError> through a
  oneshot. A failed send destroys T on the worker, but a successfully buffered T
  can later be destroyed by the receiver on Tokio. Neither path is a safe
  general disk-output handoff. Return only memory-owned replies or unit.
- WorkerPool catches a command panic and sanitizes it. It cannot preserve a
  processor moved into a panicking closure or infer its cleanup protocol.
  Catch inside the payload command boundary as described below. Pool routing
  is index modulo worker count, not thread affinity; processor state must be
  Send and may execute consecutive commands on different pool threads.
- SpoolWriter::finish consumes the writer. decode_rows borrows an immutable
  payload and opens two bounded readers. encode_rows returns an owned payload.
  PreparedReceiver owns its validator, active writer and completed parts;
  finish consumes it. PreparedResult::into_parts transfers owned files, and
  ValidatedPart::into_reader consumes the part and opens a reader. Every such
  call, error unwind and final drop belongs on the coordinator CPU/I/O pool.
- A final SpoolOperation reference can release Root through its Arc chain even
  when no file is live. Root::drop calls remove_dir. The process Spool anchor,
  operation anchors and rejected initialization inputs need the same placement
  discipline as payloads. No per-stream manager or per-attempt quota account.
- FrameReader returns None only after consuming STREAM_END and reading actual
  EOF. Bare EOF is truncation. Cancellation of a polled operation poisons the
  codec. FrameWriter::finish emits and flushes STREAM_END without supplying
  EOF; into_inner succeeds only after finish. These APIs remain unchanged.

## Private owner and finite commands

Mini 1 owns new server/src/node/payload_owner.rs and its private node/mod.rs
module declaration. Mini 2 owns new server/src/node/payload_stream.rs and its
private node/mod.rs declaration. Mini 3 extends that same stream module after
mini 2 completes. Both use the existing payload facade without changing its
row/prepared implementations. Only narrow crate-private re-exports are allowed
where later runtime wiring needs them. Tests live in private module test blocks,
so no new public facade is needed for integration tests. Existing integration
fixtures remain references, not a reason to widen production visibility.
No mini owns Cargo, cluster/state.rs, WorkerPool, frame, spool, job, auth,
controller, storage, public SDK or engine handler changes.

Pin the following private operations. Names describe the required interface;
implementation can choose normal Rust enum spelling without adding capabilities.

1. PayloadOwners::try_admit first reaps completed registry entries, then
   try-acquires the injected role-specific transport permit and retains the
   corresponding operation/dependency lifetime guard. It registers a slot
   under the same lock used by close_admission. Failure returns memory-only
   refusal and starts no task. Reaping must precede permit acquisition so a
   completed owner cannot strand the last permit. Admission uses try-acquire, with no
   unbounded semaphore waiters. Operation versus dependency-job transport
   capacities remain distinct A07 budgets, separate from operation and
   partition execution/queue admission. Acquire these bounds before spawning
   the supervisor or starting submit. Do not invent numeric settings here.
2. A registered slot holds its stable ID, cancellation watch channel, phase,
   one optional command reply, processor, and separate retained admission guards. The
   registry strongly owns every slot and supervisor JoinHandle until cleanup
   and reaping. Completed handles are reaped on admission and drain, so handle
   storage cannot grow with historical attempts. Registration and spawn are
   synchronous with no await between them. The admission lock prevents drain
   from missing a just-admitted supervisor. No public abort handle is exposed.
3. PayloadProcessor has concrete states for initial control, row writer,
   prepared receiver, validated pending result, outbound row payload, outbound
   prepared parts/current reader, and empty/disposed. It stores expected job,
   exact envelope, operation account and counters as needed. Prepared metadata
   stays incremental, with at most one active writer or reader per stream.
   Retained parts keep file charges even when they have no open descriptor.
4. Command is a closed enum: BeginRequest/BeginResult, Append, BeginPart,
   CloseRows/ClosePart, CompletePrepared, PrepareSend, ReadNext, CommitEof,
   TakeRows and Discard. Control parse/encode and PartMeta parse/encode happen
   in these commands too. Append carries one DATA buffer of at most 64 KiB;
   ReadNext returns at most 64 KiB or part/payload completion. Control/meta
   replies retain their existing 16 MiB caps. Commands perform finite local
   work; one validation command may decode one row spool or one Parquet file.
   No per-row worker submission or whole-exchange worker closure is added.
5. The supervisor serially submits a command, awaits acceptance and then
   awaits Job::receive to settlement before consuming the reply or submitting
   again. It never races away the Job on cancellation. The closure captures
   only an Arc to the registered slot plus memory-only command input. Inside
   the worker it locks the slot, borrows its processor and records a bounded
   memory-only reply. No async path takes disk state out of the slot.
6. A PayloadLease has the slot ID and cancellation sender, not disk ownership.
   Its consuming dispose requests Discard and awaits disposal acknowledgement.
   Drop records sticky cancellation under a short memory-only handoff mutex
   and notifies watch. CommitEof uses that same mutex, so its ordering with
   cancellation is defined. Awaiting result delivery has
   the same armed guard, so dropping an unpolled or pending receive signals
   abandonment. No Drop submits work, spawns a task or performs filesystem I/O.
   Ordinary Tokio watch, oneshot, Mutex and JoinHandle suffice; no new package.

The registry lock protects registration/reaping only and never spans awaits or
disk work. A slot's synchronous mutex is held only inside its one executing
command for processor access; async code uses separate cancellation/status
fields. It does not block Tokio waiting for the disk-state lock. Use a private
monotonic admission sequence modulo worker_count for routing, without an
extra queue or affinity promise.

The smallest additional ownership helper is a disposal-only slot cell, for
example Mutex<Option<ManuallyDrop<RetainedPayload>>>. RetainedPayload includes
the processor and disk/account anchors. A separate disposal-only admission cell
retains the transport permit and operation/dependency lifetime guards. Worker
commands borrow disk state through the disk cell;
Discard takes the disk wrapper and uses ManuallyDrop::into_inner on the worker,
then explicitly destroys its contents there. This needs no unsafe extraction
and no WorkerPool API change. Ordinary destruction of the cell cannot destroy its
contents on Tokio. Discard leaves the disk cell None. After Job::receive settles,
the supervisor records cleanup completion and returns without removing its
registry entry. The registry retains the slot, admission guards and JoinHandle
until a reaper observes supervisor termination and consumes its result.
Admission and drain reap completed entries before releasing their capacity.
Drain cancellation preserves every unreaped handle in registry-owned state;
it must not take the only handle into an abandonable waiter. Poll retained
JoinHandles in place, or use an equivalent cancellation-safe borrowed wait.
A cleanup notification before task termination is not completion proof: the
drain waiter must also register for actual handle termination. Failed cleanup
retains its entry and guards and cannot produce successful drain or idle.
Thus capacity remains charged through command settlement and supervisor reaping.

The wrapper is a fail-closed safeguard for violated lifecycle ordering, not
an ordinary leak-based cleanup policy. Include process Spool and final
operation-account anchors in this same explicit worker-disposal discipline.
Before submitting initialization, register a bounded empty disposal cell using
memory-only configuration and identifiers. Create and install the process Spool
or operation account inside the worker command; return only memory-owned status
or an opaque identifier. Rejected submission owns no disk anchor. Initialization
failure destroys any newly created local owners on that worker. Existing anchors
remain registered until dependent owners finish and explicit worker disposal
settles. Anchor registrations are bounded by the single process owner and
admitted operation lifetimes, including cleanup. Retained operation accounts
are shared across retries. Async admission/refusal paths accept no potentially
final raw anchor. This initialization contract constrains later runtime wiring;
mini 1 demonstrates it with its private owner fixtures. Later local producers
must deposit output into an already registered slot while still on their worker.

## Cancellation, handoff and drain

The supervisor selects on watch cancellation/deadline while doing frame I/O or
waiting for the consumer to use a delivered lease. Caller disconnect/drop sets
the same sticky flag. Cancellation abandons the codec and stops fresh frame
work; a cancelled codec is never reused. While submit is pending, retain and
await that submission future. It captures no exclusive disk owner. A command
accepted after cancellation can check the flag before doing optional work;
either way the supervisor settles it and then requests Discard. While an
accepted command runs, do not poll cancellation by abandoning Job::receive.
Observe the sticky flag when it settles. No new frame or read-next command
starts after cancellation is observed. Discard is allowed and required.

Use one command/reply at a time and no producer queue of DATA frames. Receiving
awaits Append before next_frame. Sending awaits each write before ReadNext.
Only the current frame/chunk and a bounded control/meta record can be buffered
per stream. Pool queue capacity alone does not establish this bound.

Before transport EOF, semantic completion stays in the slot as PendingEof.
For rows, CloseRows finishes the spool, calls decode_rows, stores decoded rows
and destroys the encoded spool on the worker. For Prepared, close each file
through end_part and finish through PreparedReceiver::finish. Neither exposes
success yet. Null/zero still call finish and retain their distinct outputs.

Only next_frame == Ok(None) in the grammar's terminal state permits CommitEof.
CommitEof checks cancellation and transitions to accepted under the slot's
handoff state lock. The supervisor then sends a memory-only lease ticket through
a oneshot. A failed send returns/drops only that ticket, which signals discard.
A buffered ticket dropped before its caller receives it also signals discard.
The disk output remains in the slot in both cases. The supervisor stays alive
for lease commands/disposal; its admission remains charged while output is
retained. This deliberately avoids another task or registry entry at handoff.
A later operation owner can retain the lease, read through finite commands or
transfer its use to sending. It must dispose it when done. It cannot extract
raw part/readers to Tokio. TakeRows can return memory-owned rows and then dispose
the now-empty disk state. The same command channel has one bounded slot and
permits one outstanding request; its replies never carry disk owners.

Cancellation and EOF have a defined ordering. Cancellation recorded before
CommitEof suppresses success. If CommitEof wins, cancellation can still discard
the retained accepted output, but cannot relabel incomplete input as accepted.
If delivery has already transferred the ticket, its owner now controls the
lease; drop requests disposal. Recheck cancellation before sending the ticket.
There is no promise that external disconnect and ticket delivery are atomic.
There is a promise that no disk ownership escapes and every race settles through
one Discard. Acknowledged explicit disposal occurs after that command completes.

PayloadOwners::close_and_drain closes admission under the registration lock,
signals all streams, and awaits every supervisor through command settlement,
Discard, actual termination and registry reaping before removing empty slots
and releasing admission guards. A cancelled drain waiter does not reopen
admission or abort supervisors; a subsequent drain can resume. Registry state
reports live delivery/validation/cleanup owners for later A07 idle accounting.
No stream disappears from that count when only its caller or transport ends.
Retained guards include the relevant dependency/operation lifetime protection;
this note adds no new admission or distributed drain protocol.

Shutdown order is explicit: close ingress/admission, cancel/drain payload owners
and all output leases, dispose operation accounts and the process Spool on the
still-live coordinator pool, then call WorkerThreads::shutdown and await it.
Keep the coordinator Tokio runtime alive through joining. WorkerThreads::Drop
currently starts detached joining; that fallback does not satisfy this order.
Do not rely on it for normal runtime teardown.

## Failure ownership

| Event | Required owner and action |
| --- | --- |
| submit rejected before acceptance | Registry still owns untouched disk state. Rejected closure drops only its Arc and memory input. Supervisor records failure and submits Discard if the pool can still accept it. |
| caller drops a result future or delivered ticket | Guard/ticket sends cancellation. Registry owns output; existing supervisor drains and discards it on the pool. No destructor task exists. |
| synchronous command panics | Catch unwind inside the worker while retaining slot ownership. Mark failed, discard remaining processor state on that worker and return a fixed error. Consumed local writer/reader temporaries unwind there too. Never reuse partially mutated state. |
| cancellation races with accepted completion or EOF | Await accepted work, inspect sticky cancellation, then discard or commit through the ordered handoff above. Frame EOF alone never removes ownership. |
| injected pool stops prematurely | Already accepted commands still drain under the actual pool contract, but a new Discard may be rejected. Keep slot, permits and quota in failed-drain state; report a fixed lifecycle error and refuse new admissions. Do not return successful idle/drain, free capacity, or destroy the retained cell on Tokio. |

Recover slot mutex poisoning only to dispose, never to resume parsing. The
inner unwind boundary should avoid poisoning in the normal caught-panic case.
Unexpected outer worker failure or a missing reply also enters failed cleanup;
inspect/dispose only through a subsequent worker command. If destruction itself
panics, an aborting panic, runtime loss, hung OS call or process death occurs,
this design cannot promise completed cleanup. Sanitize errors and do not claim
that catch_unwind handles abort/OOM or every destructor failure.

If premature stop makes all further commands impossible, the registered
retention cell deliberately keeps its bounded resources through process exit,
even if its outer owner is accidentally dropped. Both the disk and admission
cells suppress implicit destruction in that failure case. No fallback executor, new
thread, destructor task, recursive remover or hidden retry queue is authorized.
This is a lifecycle failure, not successful cleanup. Normal shutdown avoids it
by keeping WorkerThreads alive until registry drain and root disposal complete.
Spool unlink failures retain conservative charges as already implemented;
Discard acknowledgement means destructor execution completed, not guaranteed
physical deletion or recovered quota. Scratch gains no durability promise.

## Mini 1: R01b2b3a execution and cleanup owner

Own payload_owner.rs, its private module declaration, private owner tests and
new completion note DN-R01b2b3a.md. Deliver the concrete processor state,
disposal-only cell, try-admission/registry, cancellation guard, finite command
submission, output lease, explicit disposal and resumable close_and_drain.
Depend on actual WorkerPool and synchronous payload/spool APIs. No framing or
HTTP implementation in this mini. Reserve the command variants above so the
next minis extend concrete cases rather than add generic callback execution.

A few causal tests use an actual small WorkerPool and owned Spool directories.
Gate one accepted append/validation command with test-only start/release events,
cancel its caller and prove another try-admission still fails and quota is still
charged until the command and Discard complete. Gate only inside tests; production
commands never wait on event/network/nested work. Drop a buffered output ticket
and prove its file becomes reusable only after explicit worker cleanup. Record
thread IDs at command/disposal boundaries, including a caught command panic.

Gate a supervisor after Discard settlement but before its return. Prove that its
cleanup-complete entry still occupies admission and that cancelling a drain
waiter retains the original JoinHandle. Release that gate, resume drain and
prove reaping precedes capacity release. Separately exercise rejected anchor
initialization and a worker-side initialization failure: no raw anchor may be
created or finally dropped on the async admission/refusal path.

Saturate a worker's executing/queued positions, poll a third submit to Pending,
then stop the actual WorkerThreads. Prove rejection leaves the slot retained
and never runs its disk destructor on Tokio. This is intentionally failed drain.
A private test teardown may synchronously dispose the quarantined test cell on
its own joined cleanup thread after the assertion; it is not a production
fallback. Reuse worker_pool's existing stop/panic/drop-receiver tests rather
than reproduce routing matrices. Test normal close/drain before pool shutdown
and repeat drain after dropping its first waiter. These checks establish local
owner behavior only.

## Mini 2: R01b2b3b framed receive

Own new payload_stream.rs, its private module declaration, private receive tests
and DN-R01b2b3b.md. Extend payload_owner.rs only for concrete receive commands.
Deliver receive_request and receive_result using unchanged FrameReader. Inject
an admitted owner and expected JobEnvelope. Parse exactly one initial CONTROL
using parse_job or parse_result(expected). Parse PartMeta with parse_part_meta.
All these synchronous parsers run in owner commands. Expose an accepted request
only after terminal EOF too; no handler execution occurs in this mini.

| Receive kind | Required remainder after CONTROL |
| --- | --- |
| Request, Count, Error | STREAM_END and real EOF |
| Get, Collect | DATA*, PAYLOAD_END, STREAM_END and real EOF |
| Prepared null/zero | STREAM_END and real EOF, with semantic finish |
| Prepared n | Exactly n PART_META, DATA*, PAYLOAD_END groups, then STREAM_END and real EOF |

Rows always need an actual JSON array, including [] for absence/empty. Append
uses MAX_INTERNAL_ROW_BYTES with checked aggregate arithmetic. PreparedReceiver
supplies per-file length/hash/Parquet/range validation and incremental count
coverage. Do not add an aggregate 80 MiB prepared limit, CONTROL row parsing,
whole-array buffer or metadata replay. Reject every unexpected kind, including
repeated CONTROL, missing/extra PAYLOAD_END, early STREAM_END and extra parts.
After semantic completion the next next_frame must return None, never another
ordinary frame. Any transport/grammar/semantic failure abandons the codec and
disposes through mini 1. EOF remains pending state, never partial acceptance.

Use actual Spool, WorkerPool and Tokio duplex. One grouped grammar test covers
missing row payload, extra metadata/payload/CONTROL, a truncated frame, trailing
byte and a bad digest composed through receive. Reuse existing node_frame,
node_rows and node_prepared tests for detailed codec/scalar/Parquet matrices.
A second test finishes valid semantics, sends STREAM_END but leaves the duplex
write half open, observes Pending, cancels, and proves no accepted ticket and
admission retained until disposal. Add cancellation at a gated accepted append
using mini 1's events to connect frame consumption to command ownership. Use
fragmented DATA boundaries inside an escape/number in a small valid array.
Do not infer HTTP trailer or TCP behavior from duplex.

## Mini 3: R01b2b3c framed send and composition

Own extensions to payload_stream.rs, concrete send commands in payload_owner.rs,
private send/composition tests and DN-R01b2b3c.md. Depend on both preceding minis.
Deliver send_request and send_result with existing encode_job, encode_result
and encode_part_meta. Constructed DTOs validate before output. Rows use
encode_rows directly into the operation spool, then one bounded reader.
Prepared sending consumes already validated registered parts sequentially with
ValidatedPart::into_reader; it never returns the reader through Job or a Tokio
channel. Preserve envelope/effect, nullable count, declared part order and
exact DATA bytes. Validate outbound count/metadata consistency through the
existing validator before finishing the sequence. No new hash implementation
or second semantic validator is needed for immutable validated files.

Await each bounded DATA write before requesting another read. Emit PAYLOAD_END
for every row array and prepared file, then FrameWriter::finish. Recover the
finished underlying stream for its async owner to supply body EOF; duplex tests
explicitly shutdown the write half. Local finish proves neither peer receipt
nor framed acceptance. Disposal still follows failure/cancellation during a
write, even if final read-next has already completed. Keep quota until the last
reader is disposed. A successful send also explicitly disposes its lease.

One composed roundtrip group covers request, Count/Error, Get absence/empty
Collect and Prepared null/zero/one, asserting the distinction and exact effect.
Reuse the small real-Parquet fixture pattern from node_prepared. Another streams
the existing legal 3 MiB NUL-text Get whose escaped array exceeds 16 MiB, using
actual sender and receiver pools/Spool and without collecting encoded bytes.
This adds composition evidence to node_rows::numeric_token_bound_and_large_escaped_get,
not a duplicate scalar-boundary matrix.

Use a tiny duplex and events to hold a receiver after one chunk. Prove the
sender has no growing read-ahead queue and an unrelated finite CPU/I/O command
still runs while the network write is blocked. Drop/cancel the sender, then
release any gated accepted read command and prove admission/quota release only
after disposal. Await all supervisors and duplex tasks; use timeouts only as
watchdogs, never sleeps as causal evidence. These are local backpressure and
ownership checks, not network, cluster readiness or distributed drain claims.

## Parent handoff

No new protocol, dependency, cleanup executor or public configuration decision
is required for these three assignments. Runtime integration still must inject
and retain the separate pool, one process Spool, operation-wide accounts,
A07 role capacities and the shutdown owner in the stated order. Those are
existing parent obligations, not decisions delegated to these minis. The
explicit failed-drain retention behavior above is required if that lifecycle
is violated; no guaranteed cleanup is manufactured for an unavailable pool.

R02a later supplies the typed captured-input local handler and deposits its
sendable output into admitted ownership while still on its execution thread.
Global finalization and all-preparation-before-upload remain later orchestration
requirements. Auth, HTTP trailer/body adaptation, controller, durable mutation
completion, storage and public SDK stay outside this design. This note does
not establish new acceptance results for any of them.

Independent design review requested two essential corrections before adoption:
registry ownership must extend through actual supervisor reaping, and process/
operation anchor initialization needs an explicit worker-side ownership handoff.
The parent incorporated both above, including reaping before permit acquisition
and causal mini-1 checks. Fresh independent design re-check passed without an
essential remaining correction, and the parent adopted this boundary. No tests
or runtime guarantees follow from the design or its review.
