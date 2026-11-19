Single-key OLTP operations take about 1.7 s at RF3 because each one crosses five Raft groups over throwaway connections. Route them straight to the replicas that own the key and commit each with one replicated log entry, the way ScyllaDB serves a QUORUM write.

Status: proposed 2026-11-19 against `b29a178`. Nothing here is implemented or measured. The latency figures in the last section are targets.

## Decisions

The user decided these on 2026-11-19. They supersede the conflicting rules in [the scheduling plan](resource-aware-scheduling.md), [the shard refactor](oltp-shard-refactor.md) and Q03, Q05 and O23 for the operations listed under "Eligible operations".

| Topic | Decision |
| --- | --- |
| Replication | The leader of the key range's Data Raft group commits one log entry to 2 of 3 voters. No leaderless last-write-wins. |
| Queue | Synchronous eligible operations skip the replicated RAM queue. They are committed, or they fail and the client retries the same `request_id`. No accepted-but-pending state exists for them. |
| Request IDs | Reuse is detected per table and key, in the same commit as the row. Cluster-wide detection stays on the queued path only. |
| Rollout | On by default once every member supports it. `direct_oltp: false` restores the queued path. |

## Measured starting point

One serial `table.update` on three workers, from [the latency evidence](../docs/testing/evidence/oltp-latency/README.md) and a code trace of `b29a178`.

| Cost today | Where | After this plan |
| --- | --- | --- |
| New TCP and mTLS handshake per RPC, Raft appends and heartbeats included | [`multiplex/transport.rs`](../server/src/oltp_replication/multiplex/transport.rs) | One persistent multiplexed session per peer pair |
| About 42 RPCs and 15 Raft writes across Control, Status, Timestamp, Catalog and Data | [`worker_runtime.rs`](../server/src/oltp_runtime/worker_runtime.rs), [`transaction.rs`](../server/src/oltp_runtime/transaction.rs) | 1 Data entry, 0 or 1 coalesced Timestamp RPC |
| One entry per append, about 3 fsyncs per entry per replica | [`hosted.rs`](../server/src/oltp_replication/hosted.rs), [`pool.rs`](../server/src/oltp/pool.rs) | Batched appends, one group-committed fsync per replica |
| Four queue copy phases to every ready worker | `oltp_runtime/replica` | None |
| Gateway polls for completion, sleeping 50 to 500 ms | [`remote_api.rs`](../server/src/oltp_runtime/remote_api.rs) | Reply on the request stream |
| 88 MiB charged per request before the body is read | [`http.rs`](../server/src/oltp_runtime/http.rs) | Charge the actual body size |
| New HTTPS connection per ORM call | `orm/python`, `orm/typescript` | Keep-alive connections |

---

## Request flow

```mermaid
sequenceDiagram
    participant ORM
    participant GW as Gateway (cached topology)
    participant L as Range leader
    participant F as 2 followers
    participant TS as Timestamp leader
    ORM->>GW: POST /v1/operations (keep-alive)
    GW->>L: DirectRequest on a persistent session
    L->>L: latch key, lease check, read row, compute result
    L->>TS: AllocateDirect (coalesced, in-memory)
    L->>F: AppendEntries [DirectCommit ...] (batched)
    Note over L,F: leader and followers fsync in parallel
    F-->>L: ack from 1 follower = quorum
    L->>L: apply, release latch
    L-->>GW: exact result
    GW-->>ORM: existing response envelope
```

## Eligible operations

| Operation | Direct when | Otherwise |
| --- | --- | --- |
| `table.get` | Always | Queued path |
| `table.insert`, `upsert`, `update`, `delete` | Always | Queued path |
| `table.insert_many` | Every key maps to one range and the encoded entry is at most 1 MiB | Queued path |
| `query.collect`, `count`, `explain` | Phase 2, see "Snapshot queries" | Queued path |
| `table.create`, `describe`, `list`, `drop`, `POST /v1/requests`, receipts | Never | Queued path, unchanged |

The leader returns `Fallback` when the key holds an intent from a queued transaction or the range is mid-split. The gateway then sends that request down the queued path. Wire format, results, errors and all thirteen ORM operations stay unchanged (O01).

---

## 1. Persistent peer sessions

Replace connect-per-RPC in [`multiplex/transport.rs`](../server/src/oltp_replication/multiplex/transport.rs) and [`typed_network.rs`](../server/src/oltp_replication/typed_network.rs) with one long-lived mTLS connection per ordered peer pair and traffic class. Negotiate it with ALPN `nabedra-otp/2` and fall back to the one-shot `NABOTP01` exchange when the peer does not offer it.

```rust
// Each frame on a session. The body is the existing prologue, header and sections.
struct StreamFrame { stream_id: u64, flags: u8 /* REQUEST | REPLY | CANCEL */, len: u32 }

pub(crate) struct PeerSessions { /* (worker, incarnation, lane class) -> Session */ }
impl PeerSessions {
    async fn call(&self, to: &Endpoint, lane: Lane, request: Frame, deadline: Instant) -> Result<Frame, TransportError>;
}
```

- Verify the certificate pin, ALPN and `directory.verify` once per connection. Close a session when its peer's incarnation leaves the local directory.
- Keep the per-lane semaphores as limits on concurrent streams. Raft progress and control lanes keep their protected capacity (O05, C07).
- Set `TCP_NODELAY` and write each frame with one vectored write.
- Cache a receiver's `service_role` and `service_grant` result per caller incarnation and role. Drop the entry when the locally applied Control revision changes or after 5 s. A revoked caller can therefore act for at most 5 s. Data safety does not depend on this cache, because every write checks its fence in the Data log.
- A request that fails after the frame was written has an unknown outcome, as today.

## 2. Raft batching and group commit

In [`hosted.rs`](../server/src/oltp_replication/hosted.rs) replace `single_entry_config` with byte-bounded batching: at most 64 entries and 1 MiB per append, `api_batch_linger_ms = 0`. Then change the three storage calls.

| Write | Today | Change |
| --- | --- | --- |
| Log append in [`typed_log.rs`](../server/src/oltp_replication/typed_log.rs) | One synced `WriteBatch` per entry | One synced `WriteBatch` per append batch. RocksDB group-commits concurrent syncs from other groups on the same pool. |
| `save_committed` | Synced | Unsynced. OpenRaft treats it as a recovery hint. |
| Apply in [`pool.rs`](../server/src/oltp/pool.rs) `mutate` | One synced batch per entry | One unsynced batch for all entries in the apply call, with position, reply and membership inside it. |

An unsynced apply is safe because the log and the state share one RocksDB WAL. A crash loses only a suffix of the WAL, and the surviving log replays what the lost applies covered. Purge the log and build snapshots only up to an applied index that a later synced write has made durable. Keep `AbsoluteConsistency` WAL recovery. Rerun O06, O11, O29 and the Raft-append cancellation regression on this change alone before building on it.

Sections 1 and 2 also speed up the queued path. Benchmark them alone as a checkpoint.

## 3. Topology cache

Add `oltp_runtime/application/direct.rs` on the gateway.

```rust
struct TopologyCache { /* bounded LRU */ }
struct TableRoute { table_id: Uuid, key_type: KeyType, schema_epoch: u64, ranges: Vec<RangeRoute> }
struct RangeRoute { span: KeySpan, group: GroupId, routing_epoch: u64, voters: [Endpoint; 3], leader_hint: Option<WorkerId> }
```

Fill an entry from one Catalog read and one Control placement read on a miss. Refresh it only when a worker answers `StaleRoute { schema_epoch, routing_epoch }`, `NotLeader { hint }` or `UnknownTable`. The steady state makes zero Control or Catalog calls per request. A cached route never authorizes a write. The leader and the log entry check epochs and fences.

## 4. One-entry commit

Add `server/src/oltp/mvcc/store/direct.rs` and one `ParticipantCommand::Direct(DirectCommit)` variant in [`store.rs`](../server/src/oltp/mvcc/store.rs).

```rust
pub(crate) struct DirectCommit {
    authority: Authority,        // run and group fence, checked at apply
    table_id: Uuid,
    schema_epoch: u64,
    routing_epoch: u64,
    request_id: Uuid,
    digest: Digest,              // SHA-256 of the exact request body
    commit_ts: CommitTs,
    writes: Vec<DirectWrite>,    // 1 for point operations
    result: RawValue,            // exact `data` value of the response
}
pub(crate) struct DirectWrite {
    key: MvccKey,
    expected_version: Option<CommitTs>,   // None = key has no visible row
    value: Option<RawValue>,              // None = tombstone
    index: Vec<IndexChange>,
}
pub(crate) enum DirectReply { Applied { commit_ts: CommitTs }, Replayed { result: RawValue }, Rejected(Fault) }
```

Apply validates in this order and writes nothing on failure: the group is not frozen for a split, `authority` matches the current fence, both epochs match the group config, every key belongs to the group, no key has an intent, each `head.version == expected_version`, and `commit_ts` is above each key's newest version. It then writes the `'v'` versions, the `'h'` heads, the index entries and one dedup record in the same batch as the applied position.

```text
dedup key   = 'D' ‖ table_id ‖ anchor primary key ‖ request_id     anchor = smallest key in the request
dedup value = { digest, commit_ts, result, applied_unix_ms }
```

An existing dedup record with the same digest returns `Replayed`. A different digest returns the existing `conflict` error. Dedup records sort with their anchor row, so splits, snapshots and backups must carry the `'D'` namespace. A background task deletes records older than `direct_dedup_retention_ms`, default 24 h. Read-only outcomes such as `update` on a missing row commit nothing and are recomputed on retry.

**Mixed versions.** Add a Control capability `data.direct.v1` next to the existing [scoped-queue capability](../server/src/oltp_replication/authority/control/machine/scoped_queue/capability.rs). Control activates it automatically when every voter and learner advertises it. No leader proposes `Direct` before activation, since an older replica cannot decode the variant.

## 5. Commit timestamps

Direct commits need timestamps from the same domain as queued snapshots, so cross-shard snapshot reads stay consistent. Today every allocation is a Timestamp Raft write. Add a durable window in [`authority/timestamp.rs`](../server/src/oltp_replication/authority/timestamp.rs).

```rust
TimestampCommand::ReserveWindow { authority: Authority, count: u64 }   // high_watermark += count, replies with the range
```

The Timestamp leader hands out values from its reserved window in memory through a new `AllocateDirect { count }` service method, after `ensure_linearizable(ReadPolicy::LeaseRead)`. Applying any other command that moves `high_watermark` discards the rest of the window, so issued values stay monotonic in real time across both paths. A new leader starts above the durable watermark. Each worker coalesces concurrent `AllocateDirect` callers into one RPC, and a leader co-located with the Timestamp leader calls in process.

Each range leader keeps this state in memory.

```rust
struct RangeClock { max_read_ts: ReadTs, pending: BTreeMap<CommitTs, PendingKeys> }
```

1. A direct commit takes its timestamp after latching its keys. If `commit_ts <= max_read_ts`, it fetches a new one. It registers in `pending` before proposing and leaves on apply or rejection.
2. Every snapshot read at `s` on the leader, direct or queued, raises `max_read_ts` to `s` and waits until no `pending` entry is at or below `s`. Add this to `read_at`, `read_page` and the scan path in [`store/handle.rs`](../server/src/oltp/mvcc/store/handle.rs).
3. A new leader serves nothing until its first entry of the new term applies. That empties `pending`.

Rule 1 keeps a commit from landing below a snapshot that already read the range. Rule 2 keeps a reader from missing a commit still in flight (O07, O09). Queued prepares already conflict when a newer committed version exists, so they see direct commits as ordinary committed versions.

## 6. Leader execution

Add `oltp_runtime/direct/` with one service method on the ingress lane.

```rust
Method::Direct(DirectRequest { database, table_id, schema_epoch, routing_epoch, group, request_id, digest, body, deadline })
enum DirectResponse { Done(RawValue), Failed(ProtocolError), Fallback, StaleRoute { .. }, NotLeader { hint } }
```

Mutation steps on the leader:

1. Charge the body bytes and one in-flight slot against the worker's existing budgets. When full, return `429 overloaded` with `not_applied`.
2. Check leadership and both epochs. Latch the keys in sorted order in a striped `KeyLatches` table, bounded by the deadline.
3. Look up the dedup record. Read each row head and newest version from local applied state.
4. Validate the row against the cached table descriptor. Compute the new row, the index changes and the exact result, reusing the planner's row and index functions from [`planner.rs`](../server/src/oltp/mvcc/planner.rs).
5. Allocate `commit_ts`, register in `pending`, propose `DirectCommit` and await apply.
6. On `Rejected(Conflict)` from a leadership race, go back to step 3 while the deadline allows. Release the latches and reply.

`table.get` runs steps 1 and 2 without latches, then `ensure_linearizable(ReadPolicy::LeaseRead)`, falling back to `ReadIndex` when the lease is stale. It reads the newest version and returns `Fallback` on an intent. It needs no timestamp. It waits for any `pending` commit on its key.

The gateway retries `NotLeader` and `StaleRoute` against the refreshed route. After a lost reply it resends the same `request_id` and body, and the dedup record makes the retry exactly-once. When the deadline expires, it returns `outcome_unknown`. Reduce the ingress charge in [`http.rs`](../server/src/oltp_runtime/http.rs) to twice the body length for direct requests.

## 7. Schema changes

The direct path takes no Catalog lifecycle guard, so `table.drop` and index publication must stop direct writers in the Data log. Before the Catalog publishes either change, the DDL coordinator commits the new `schema_epoch` to every Data group of the table through the existing `Fence` command. A `DirectCommit` carrying the old epoch then fails at apply, and the gateway refreshes its route. The implementer must confirm which DDL paths already bump the group epoch before publication and add the step where one does not. Run O08 and O12 with direct writers racing each DDL.

## 8. ORM connections

Reuse HTTPS connections in both ORMs, in the same PR and under one version bump from `0.2.0`, per [the parity rule](../docs/testing/orm-parity.md). Python keeps a small pool of `http.client.HTTPSConnection` objects per `_Transport` behind a lock. TypeScript defaults to an `https.Agent` with `keepAlive: true`. Neither ORM gains a retry. Confirm the server keeps HTTP/1.1 connections alive.

## Snapshot queries (phase 2)

The gateway takes one `AllocateDirect` timestamp `s`, sends `DirectScan { span, s, plan }` to each intersecting range leader in parallel, and merges with the planner's bounded merge. Leaders apply rule 2. Any intent or split returns `Fallback` for the whole query. Phase 2 starts only after phase 1 passes its tests and benchmark.

## Scaling

A point operation touches three voters regardless of cluster size. Throughput grows with the number of ranges and how evenly their leaders are spread. New tables start as one range, so one worker leads all of a small table's writes. The existing automatic split creates more ranges under load. Add a Control task that moves range leaders until per-worker leader counts differ by at most one. Measure 3 and 6 workers before claiming a scaling factor (O25).

---

## Failure behavior

| Event | Result |
| --- | --- |
| Gateway or client dies mid-request | The entry commits or it does not. A retry with the same ID and body returns the one result. |
| Leader dies before quorum | The gateway retries on the new leader. The dedup record decides whether the first attempt committed. |
| One follower slow or down | Commits continue on the leader and the other follower. |
| Two voters down | No commit. The gateway returns `outcome_unknown` or unavailable at the deadline. |
| All workers stop | Committed rows and dedup records recover from disk. Nothing pending exists to abandon. |
| Split or leader move | `StaleRoute` or `NotLeader`, one cache refresh, retry within the deadline. |
| Queued transaction holds an intent on the key | `Fallback` to the queued path. |

## Tasks

Cite the scenario IDs in test evidence. Tasks 1, 2, 3 and 8 have no dependencies and can run in parallel.

| Task | Deliverable | Depends on | Scenarios |
| --- | --- | --- | --- |
| T1 | Persistent peer sessions, cached grants | None | O05, O06, C07, D10 |
| T2 | Raft batching and group commit | None | O06, O11, O29, D09 |
| T3 | `ReserveWindow`, `AllocateDirect`, coalescing client | None | O09, D06 |
| T4 | `DirectCommit`, dedup records, `RangeClock` read rules | T3 types | O07, O09, O11, D03, D06 |
| T5 | Leader executor, latches, admission, `Method::Direct` | T3, T4 | O03, O08, O13, D01 to D05 |
| T6 | Gateway topology cache, routing, retry, fallback, ingress charge | T5 types | O03, O16, D01, D07, D08 |
| T7 | `data.direct.v1` capability, `direct_oltp` switch, DDL fence | T4 | O02, O08, O12, D11 |
| T8 | ORM keep-alive in Python and TypeScript, version bump | None | O01, parity checks |
| T9 | D01 to D12 tests, existing suites, RF1 `serve` | T5 to T7 | All D, O01, U01 |
| T10 | Benchmark runner changes, paired runs, evidence | T9 | O24, B01, B02 |
| T11 | Phase 2 queries, leader balancing | T10 | O04, O25 |

## Tests

Add these to [the OLTP contract](../docs/testing/oltp-contract.md) under "Direct point protocol". Use real workers over mTLS for every row except D06.

| ID | Scenario | Required assertions |
| --- | --- | --- |
| D01 | All eligible operations through both ORMs at RF1 and RF3 | Results, errors and types equal the queued path for the same program. Stats show zero queue copies and one Data entry per mutation. |
| D02 | Kill the gateway after the leader proposes, then retry the same ID and body through another gateway | One committed version, the original result, one dedup record. |
| D03 | Same ID with a different body on the same key, then on a different key | The first is a conflict. The second executes, as decided above. |
| D04 | Kill the leader before quorum and after quorum | No acknowledged write is lost and none applies twice. The retry completes on the new leader. |
| D05 | Pause one follower, then two | One paused: writes succeed within the deadline. Two paused: no success response and the row is unchanged. |
| D06 | A snapshot scan at `s` races direct commits, including across a Timestamp leader change | The scan sees every commit at or below `s` and none above. No commit appears below a snapshot that already read the range. |
| D07 | Split a range and move its leader during a write load | Each request gets one outcome, with no lost or duplicate rows. Stale routes refresh once. |
| D08 | Direct and queued writers on the same keys, including an intent held open | Histories match a serial order. Direct requests fall back while the intent exists. |
| D09 | Kill all workers under write load, then cold start | Every acknowledged write is present. Dedup records replay acknowledged IDs. |
| D10 | Drop sessions mid-stream, exhaust a lane, offer an old one-shot peer | Callers see unknown-outcome or overload errors, Raft progress continues, and the old peer still interoperates. |
| D11 | Drop a table and publish an index during direct writes | No row lands in a dropped table and no write uses a retired schema epoch. |
| D12 | Offer more load than the leader's in-flight budget | Explicit `429` before execution, bounded memory, and every acknowledged ID verified afterwards. |

Also run the existing OLTP library suite, the four OLTP integration tests, `scripts/acceptance_oltp.py`, both ORM suites and the scoped acceptance runners with `direct_oltp` on and off. The five native failures recorded in [the scoped evidence](../docs/testing/evidence/oltp/scoped-queues/README.md) are the known baseline.

## Benchmark

Compare three targets on the same host, one at a time, under the existing 4 CPU and 7.9 GB cap: `b29a178` release, the candidate release, and ScyllaDB 6.2.3 at RF3, QUORUM, batch commit-log sync.

- Update [`benchmark_oltp_nabedra.py`](../scripts/benchmark_oltp_nabedra.py) so it no longer requires queue-owner stage stats. Add a `direct` section to `/v1/runtime/stats` with entries proposed, batch sizes, fallbacks and retries. Add `--direct-oltp on|off`.
- Run the serial pair unchanged for continuity: `--rates 0.2 --seconds 5 --warmup-seconds 5 --repetitions 10 --clients 1 --rows 2000 --seed 20261118 --timeout 10`.
- Run the sweep `--rates 1,2,5,20,50,100,200,500,1000` and continue until p99 exceeds 100 ms or rejections begin. Add a point-get sweep and a 95/5 read/write mix.
- Report scheduled-arrival p50, p95 and p99, acknowledged throughput, rejections, timeouts, unresolved outcomes, CPU throttling and fsync counts. Verify the snapshot after each phase.
- Checkpoint after T1 and T2 with `--direct-oltp off` to show what transport and batching contribute alone.

Targets, in order: serial update p50 at or below 15 ms; zero rejections through 200 updates/s with p99 at or below 25 ms; then the ±20% band from [the shard refactor](oltp-shard-refactor.md). Nabedra's `update` reads the row before writing and returns whether it existed, which ScyllaDB only does with lightweight transactions. Report that difference next to the numbers.

## Out of scope

Removing the queued point path or the scoped queue from PR #19. Both stay in the tree, and the scoped queue stays off by default. A thread-per-core runtime. Hash partitioning. Follower reads. Cross-range one-phase commit.
