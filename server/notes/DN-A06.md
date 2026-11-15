# DN-A06: R01b2b semantic payload boundary

Design only, accepted by the parent for implementation on 2026-11-11.
No implementation or test completion is asserted.

Parent reconciliation: adopt the two-pass resource guard and strict seeded
decoding below. Internal row arrays limit each numeric token to 128 bytes;
public request parsing is unchanged. This admits every current i64/f64 producer
serialization, including signed zero, without admitting unbounded numeric-token
allocation. Accept direct `sha2 = "=0.11.1"` for prepared-file hashing; the version
is already cached and locked. The implementation owner may add only that direct
dependency and its root lockfile dependency entry, without fetching packages.
Use the bounded coordinator CPU/I/O pool and supervised cancellation ownership
described below. Split the first proposed mini into row semantics (R01b2b1) and
prepared-file semantics/processor ownership (R01b2b2), followed by the framed
adapter (R01b2b3) and typed local seam (R02a). This keeps assignments small;
it does not remove any validation or acceptance requirement.

Accepted [A09](DN-A09.md) refines the executor ownership wording below against
the implemented WorkerPool API. Commands borrow disk-backed state from bounded
registered disposal cells only after execution starts; they never transfer its
exclusive ownership through a pending submit closure or Job result. This
supersedes the illustrative processor-state transfer wording below. A09 also
pins worker-side anchor initialization, retention through supervisor reaping,
and separate owner/receive/send minis. Semantic and framing limits stay unchanged.

Scope is the next payload slice over reviewed `node/job.rs`, `frame.rs`, and
`spool.rs`. `plans/distributed-contracts.md`, Execution transport and validation,
and DN-R01a/R01b1/R01b2a are authoritative. Historical A01 embedded rows,
embedded replacement vectors and no-op journals do not apply. HTTP, auth,
membership, retries, publication, runtime configuration and actual TCP worker
acceptance remain later work.

## Stream acceptance

The adapter owns each codec for one attempt. The following are exact sequences.
`DATA*` means a sequence of nonempty bounded frames, with a separately checked
aggregate length. It does not permit an empty JSON payload.

| Stream | After the initial CONTROL |
| --- | --- |
| Request | STREAM_END, real EOF |
| Count or job error | STREAM_END, real EOF |
| Get or Collect | DATA*, PAYLOAD_END, STREAM_END, real EOF |
| Prepared with count n | n repetitions of PART_META, DATA*, PAYLOAD_END; then STREAM_END, real EOF |

Use `parse_job`, `parse_result(expected_job)`, and `parse_part_meta` rather than
deserializing their DTOs directly. Validate constructed outbound DTOs through
their existing encoders. Identity includes attempt and captured snapshot, not
just job ID. Prepared null means unchanged; zero means changed to empty. Both
have no metadata or payload frames. Preserve this distinction in logical output.

Reject an unexpected frame in every state, including another CONTROL, an extra
PAYLOAD_END, metadata during rows, and payload after the declared final file.
Missing `[]`, incomplete JSON, a second JSON value and non-whitespace trailing
JSON bytes all fail. JSON whitespace inside the payload is allowed and charged
against its encoded cap. A completed JSON value alone never accepts an attempt.
Only `FrameReader::next_frame() == Ok(None)` proves STREAM_END plus real EOF.
Any earlier failure abandons and drops the owned codec, including semantic
failure between frames when the underlying codec itself is still Ready.
Cancellation during a polled frame operation retains the codec's poisoning rule.

## Rows without an encoded-array allocation

Spool the DATA bytes first using one `SpoolWriter`, capped at
`MAX_INTERNAL_ROW_BYTES = 6 * 64 MiB + 2`. Check checked additions before append.
Do not use `Vec<u8>` for the aggregate or call `parse_get_rows` on a collected
buffer. After PAYLOAD_END, finish the spool and validate through bounded readers.
The sender serializes rows directly with `serde_json::to_writer` into a bounded
spool sink that splits writes into at most 64 KiB appends. It subsequently sends
fixed-size reads as DATA. This preserves the approximately 384 MiB escaped
allowance without a network buffer or encoded-array allocation of that size.
Disk budgets may refuse admission, but must allow an individually legal maximum
array in a configured operation/process budget; do not substitute 16 MiB or
the public 8 MiB response cap.

Actual decoding needs new private `DeserializeSeed` visitors, not derived Row.
`Row` is a BTreeMap alias and overwrites duplicates. `Scalar::deserialize` first
allocates `Box<RawValue>` and then a Value; using it for a giant escaped string
would allocate its entire raw spelling. `job::strict` also captures raw subtrees
and applies the CONTROL cap. Neither is the streaming row validator.

Recommended smallest robust strategy is two sequential bounded-reader passes:

1. A private lexical resource guard scans only the flat array-of-row-objects
   format. It retains counters and token state, no token strings or value tree.
   Count decoded UTF-8 bytes of string tokens, including escapes and surrogate
   pairs. Field names are at most 64 decoded bytes because descriptor identifiers
   already have that bound. Charge row overhead, cell overhead, field names and
   scalar sizes with the exact `snapshot::row_payload` formula: 8 per row,
   8 plus name length per cell, UTF-8 value length, 8 per number, 1 per bool.
   Reject excess rows and decoded totals before the decoding pass allocates
   strings/maps. Precharge each row's known schema overhead before its values.
   Keep key/value context only for this fixed shallow format; reject nested
   containers in scalar positions. Serde remains responsible for JSON syntax,
   Unicode validity and semantic construction. This guard is not a general JSON
   parser, canonicalizer or replacement for Serde. Malformed/incomplete token
   state fails closed. Do not count six raw bytes of `\u0000` as six decoded bytes.
2. Reopen the immutable spool and use `Deserializer::from_reader(BufReader)`
   with an outer sequence seed and an object-only row seed. Resolve field names
   against the schema and reject unknown/duplicate fields before `next_value_seed`.
   Reject missing fields at row end and surplus rows before building them.
   For Utf8, deserialize a String directly rather than via Scalar/RawValue;
   the first pass has already bounded its decoded size. For numeric/bool tokens,
   use bounded RawValue and the existing Scalar decoder, then require the exact
   schema variant. This preserves integer versus decimal/exponent syntax,
   i64 range, finite f64, float negative zero and rejection of null/containers
   and Serde private-marker objects. Finish with `Deserializer::end()`.

Parent wire decision required: recommend a 128-byte maximum numeric token in
internal row arrays, enforced by the resource guard before RawValue allocation.
Existing `Scalar` accepts arbitrarily long finite numeric spellings such as a
zero with thousands of exponent digits; Serde's arbitrary_precision reader can
allocate those spellings before a visitor can reject them. A post-deserialization
check does not solve that problem. The proposed bound admits existing i64/f64
producer serialization, including signed zero, and leaves the full string and
decoded-table allowances intact. It is an explicit restriction on alternative
numeric wire spellings, not an existing shared-contract rule. Parent must adopt
and document it before implementation. If unrestricted spellings must remain,
return this decision to design rather than silently imposing a token cap or
adding a streaming numeric parser. No canonical field order or string escaping
restriction is required.

Validate each complete row using `protocol::validate_row(Complete)` and keep
checked accounting/typed key uniqueness across the array. Reuse
`snapshot::validate_rows` as the final authority over the bounded decoded rows;
its payload budget bounds logical data, not allocator overhead. Do not reserve
based on untrusted counts. Small Vec growth, one Serde string scratch buffer,
decoded rows and key tracking are allowed; no complete raw scalar-string copy
or raw row tree is allowed. Process concurrency must account for this decoded
working set as well as spool disk quotas.

Get requires exactly `usize::from(found)` rows, complete schema, requested key,
and decoded payload no greater than source_payload_bytes. Reuse
`validate_get_rows` after decoding. Collect requires exactly matched_rows full
unprojected rows, all matching the query, with unique primary keys and the same
local ordering as `query::local_collect`: order_by direction followed by primary
key ascending, or primary key ascending alone. Expose narrow crate-private
predicate/comparator helpers rather than copying comparison semantics. Never
apply select, limit, public result count or public response size here.

Use existing result validation for source_rows/source_payload_bytes, including
captured file counts, partition payload metadata and 100,000 rows/64 MiB bounds.
Returned row totals must fit those source totals. The coordinator later checks
aggregate source totals even when filtering returns nothing. The receiver can
check every returned row and the declared counts; it cannot prove that a worker
omitted no matching row without executing the source job again. That correctness
comes from the shared pure handler, not from transport authentication or parsing.
Global order, limit and projection occur once at the coordinator. Count carries
the unlimited local match count, summed before the global limit.

## Prepared files and hashing

Create `PreparationValidator` from the validated result/job. For each PART_META,
parse it and immediately `push` it before opening that file's writer. This checks
count, ordered contiguous range coverage and checked aggregate counters with
the existing 110,000 pre-finalization rows/files limit. Call `finish` even for
null/zero. Do not collect a metadata array in CONTROL or impose an aggregate
80 MiB encoded cap. Retained handles/metadata may be indexed in a bounded Vec
up to the declared limit; file bytes stay in spools and closed handles hold no FD.

Use a writer capped at that PartMeta.byte_size. Incrementally hash exactly the
DATA payload bytes, excluding frame headers/terminators/metadata. At PAYLOAD_END
require actual length == byte_size and SHA256 == content_digest. Use lowercase
64-character hex; parse/encode metadata through the existing helpers for
required nullable ranges, canonical field names and validation. Hash raw bytes,
never re-encoded Parquet or serialized metadata.

Narrow dependency proposal for the parent: add direct `sha2 = "=0.11.1"` and
update only the root dependency list in Cargo.lock. That version and its source
are already present in the lock/cache. Its inspected API exports
`sha2::{Digest, Sha256}` with new/update/finalize. A transitive dependency is not
directly importable. This design makes no dependency edit and proposes no new
crypto implementation or dependency fetch. Hex formatting can use standard
formatting over the 32 digest bytes without another dependency.

After length/digest checks, validate one completed file at a time. For this slice
read at most its 64 MiB into Bytes and reuse `snapshot::decode(table, bytes,
Some(meta.row_count))`. It already runs envelope/footer and raw schema guards,
physical/Arrow schema checks, page/payload preflight, exact row counts, complete
rows, finite values and uniqueness. This bounded single-file copy avoids a new
Parquet reader abstraction. Check `snapshot::validate_rows`' returned payload
against meta.payload_bytes, and check every typed key against meta.range plus
strict ascending order. These last checks currently live in
`engine::partition::load_file`; extract a small shared validation helper rather
than bypassing them or copying the loader's storage fetch. Drop decoded rows and
Bytes before validating the next file. Never collect all decoded files.

Keep all validated spools private until the entire stream and metadata sequence
finish. Later coordinator finalization combines replacements with unaffected
files, fixes empty-range routing as the current write path does, and checks global
100,000 rows, 64 MiB decoded payload and catalog encoding limits. No table-object
uploads may start until all jobs, file checks and global validation succeed.
Scratch is ephemeral and neither a publication journal nor a no-op decision.

## Types, scheduling and cancellation ownership

Return an `AcceptedResult` containing the validated ResultEnvelope and a matching
logical enum: Get optional Row, Collect full Vec<Row>, Count scalar, Prepared
effect plus `Option<Vec<ValidatedPart>>`, or sanitized JobError. `None` versus
`Some(empty)` preserves unchanged versus changed-empty. Each `ValidatedPart`
privately owns PartMeta and SpoolPayload; its constructor is private. Offer a
consuming bounded-reader/upload adapter for later use, not paths, File, FDs or
unrestricted public clones. Rows need no retained encoded spool after acceptance.
Outbound `SendableResult` similarly owns validated row/file payload handles.
No Debug or error may reveal paths or payload/parser/OS text.

One process Spool supplies per-operation quotas shared across all attempts of
that operation. The quota remains charged while a writer, payload or reader
survives. Limit concurrently open readers independently of retained file slots;
normally use one per active send/validation command. No per-job Spool managers.
Drop cleanup can unlink files synchronously, so ownership placement includes
destruction as well as reads/writes.

Use a synchronous `PayloadProcessor` with finite commands such as start,
append, close-and-validate, read-next-chunk and discard. Run commands on a bounded
coordinator CPU/I/O `WorkerPool` instance, separate from partition execution.
Reuse the existing dedicated threads/current-thread runtime and bounded queues;
do not change FrameReader/Writer or introduce per-chunk spawn_blocking.
Each async adapter reads a frame, transfers processor state and that frame to
one submitted command, and awaits its result before reading another. Sending
requests one bounded chunk, then awaits frame output. Workers perform synchronous
disk/Serde/Parquet work outside runtime.block_on and never wait for peer frames,
peer jobs or nested pool submissions. Thus slow network delivery occupies a
transport slot, not a partition worker or partition permit.

Admission distinguishes an operation slot, a partition execution slot, and a
bounded transport/validation slot. Acquire transport admission before creating
its supervisor or waiting on pool submission so pending closures cannot grow
without bounds. WorkerPool alone does not bound the number of callers waiting
in `submit`. Its per-worker queue has capacity one, and dropping Job::receive
does not cancel accepted work. Operation orchestration awaits peer results on
the coordinator runtime without holding partition permits. The partition closure
owns its permit until its CPU/disk work actually ends. Do not queue whole remote
round trips onto the partition pool.

An admitted stream has one supervised async owner with a cancellation signal
and deadline. Cancellation stops new frame work, abandons the codec, then drains
the currently accepted command. Check cancellation between chunks/rows/files;
a synchronous decode may finish before cancellation is observed. Keep the
transport permit until that command and disk cleanup finish. The supervisor
must survive caller disconnect and await commands itself; aborting a JoinHandle
is not cancellation of disk work. Returned processor state and rejected outputs
are discarded on the CPU/I/O executor, including final-reader/payload drops.
No destructor starts an unbounded cleanup task or blocking queue. Shutdown drains
supervisors before WorkerThreads shutdown. Pool startup and numeric capacities
remain injected runtime choices, not new configuration in this slice.

The pure worker job owns candidate output until transferring it to the delivery
owner. Partition capacity can then be released because computation has ended;
the output's spool quota stays charged during delivery. A disconnected delivery
owner cancels/drains its command and drops output on the bounded executor. A
disconnected pure job may discard results when computation ends. Neither case
requires the durable-mutation completion rule or a result cache. Accepted output
transfers to the operation owner with the same executor cleanup obligation.

R02's local handler must produce these same logical outputs using captured
inputs, `partition::load_file`, `query::local_collect/local_count`,
`mutation::apply` and `snapshot::encode_partition_range`. Existing mutation
application/encoding checks remain in force. Do not call public Engine methods
that load a fresh catalog or apply final projection/limit. Reuse constructors
and semantic validators for local and remote results; local delivery needs no
JSON frame round trip. R01b2b pins this seam, not the R02 dispatcher/runtime.

## Next mini assignments

Run sequentially as bounded Sol-high assignments. The parent owns decisions,
integration and any later review assignments. No delegation occurs in this design.

1. **Semantic payloads.** Own new `server/src/node/payload.rs`,
   `server/src/node/payload/rows.rs`, `server/tests/node_payload.rs`, the payload
   export in `server/src/node/mod.rs`, and the narrow sha2 manifest/lock change
   after parent approval. Own only the helper extraction in
   `server/src/engine/partition.rs` and crate-private predicate/comparator access
   in `server/src/engine/query.rs`/`engine.rs`. Depend on reviewed job/spool.
   Deliver opaque handles, finite processor commands, guarded Serde decoding,
   one-file Parquet validation and hashing. Keep job/frame/spool unchanged.
   Small causal tests use actual Spool: duplicate/unknown/missing fields,
   scalar syntax and signed-zero bits, key/filter/order/accounting/count failures,
   numeric guard edges, escaped Get above 16 MiB, valid Parquet and separately
   wrong digest/length/schema/row count/range. Exercise tiny injectable guard
   budgets to show rejection before large token allocation; retain production
   constants. No 384 MiB buffer or giant table matrix.
2. **Framed adapter.** Own new `server/src/node/payload_stream.rs`,
   `server/tests/node_payload_stream.rs`, and its node/mod.rs export. Depend on
   mini 1. Deliver async request/result assembly and sending over unchanged
   FrameReader/Writer, plus the supervised finite-command WorkerPool adapter
   with injected admission/cancellation. Test actual Spool plus Tokio duplex,
   fragmentation inside escapes/numbers, empty arrays, streamed Get above
   16 MiB, round-trip Prepared null/zero/one, missing/extra payload, bad digest,
   truncated lengths, STREAM_END without EOF and trailing bytes. Use controlled
   events to cancel during an accepted disk command and while awaiting EOF;
   prove capacity/quota survives until command cleanup and no partial success.
   Await both duplex owners, use deadlines only as watchdogs, and explicitly
   supply writer shutdown after finish to create EOF.
3. **Typed local seam.** Own new `server/src/engine/job.rs`, its private module
   declaration/narrow crate-private facade in `server/src/engine.rs`, and
   `server/tests/node_handler.rs`. Depend on minis 1 and 2. Add only the captured
   job-to-logical-output handler seam and spool-backed outbound conversion,
   without changing public read/write/publication paths. Tests use a small
   existing fake store: stale captured identity rejection, full local Collect
   rows despite select/limit, unlimited Count, Get absence, and Prepare unchanged
   versus changed-empty. Feed a small local output through the framed adapter
   and validate it with the same validators. Assert no object writes. Whole
   distributed handler scheduling and TCP/multiworker acceptance remain R02/later.

Mini owners run only their focused causal checks under the parent's existing
Cargo resource policy. This note records source inspection, not execution of
those proposed checks.
