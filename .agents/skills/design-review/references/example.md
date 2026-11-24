# Sample design rubrics

Each rubric picks 3 to 8 properties per plan, the faults that can break
them, and one mutant per invariant. They show the shape of a rubric. They
are not checked specs, and none of their properties has run.

## OLTP direct path

Plan: [OLTP direct path](../../../../plans/oltp-direct-path.md). Module
`oltp`, level L3.

**Topology.** ORM client, gateway with cached topology, range leader, two
followers, Timestamp leader. Channels: client to gateway, gateway to
leader on a persistent session, leader to followers for AppendEntries,
leader to Timestamp leader for `AllocateDirect`. Stores: each replica's
Data log, synced and unsynced.

**Faults.** Leader crash after quorum and before the reply. Lost reply to
the gateway or the client. A leader move while the gateway's topology is
stale. A follower partition. A retry racing its original. A retry with a
changed body. Torn unsynced log tail on restart. Out of scope: Byzantine
replicas and disk corruption of synced entries, since checksums are a
storage-engine guarantee.

**Bounds.** 3 replicas and 2 ranges, 2 clients, 2 keys, 1 crash, 1 lost
reply, 1 partition and 1 leader move, with the crash and the leader move
overlapping. Exhaustive, no state cap.

| Guarantee | Property | Mutant that must break it |
| --- | --- | --- |
| OLTP-G03 | `G03_OncePerKey`: for each table, key and request ID, at most one committed Data entry | `dedup_outside_commit`: the request-ID record is written in a separate entry from the row |
| OLTP-G03 | `G03_SameResult`: a retry with the same ID and body gets the original result | `reexecute_on_retry`: a retry after a lost reply runs the operation again |
| OLTP-G04 | `G04_NotAppliedHasProof`: a `not_applied` reply implies no entry with that ID is committed or can commit | `timeout_not_applied`: the gateway replies `not_applied` on a timeout |
| OLTP-G06 | `G06_NoRetiredLeaderSuccess`: a leader whose lease has expired never replies success | `skip_lease_check`: the leader replies without the lease check |
| OLTP-G02 | `G02_PerKeyRealTime`: committed writes to one key are ordered by timestamp in their real-time order | `stale_window`: a new Timestamp leader issues values below the durable watermark |
| OLTP-G10 | `G10_Progress`: with a stable quorum and a retrying client, every request gets a definitive reply, under weak fairness on append, retry and heartbeat | `no_retry_after_conflict`: the leader replies on `Rejected(Conflict)` without retrying |

**Cost.** From the spec's actions, per write: 1 Data entry, 1
AppendEntries round trip, 0 or 1 `AllocateDirect` RPC, 1 fsync per
replica. Compare with the plan's table: about 42 RPCs and 15 Raft writes
today.

**Rollout.** Nodes with and without direct-path support, and the
`direct_oltp` flip in both directions, as actions. `G03_OncePerKey` holds
when a request first seen on the queued path is retried on the direct path
after the flip.

**Not modelled.** OLTP-G13, latency targets: timing, measured with `ab.py`.
OLTP-G12, ORM parity: client encoding.

## Cache lease failover

Plan: [Cache](../../../../plans/cache.md), "Leases" and "Recovery during
replica replacement". Module `cache`, level L2.

**Topology.** Three controller voters, a primary and a replica per slot
group, clients. Channels: controller to workers for grants, primary to
replica for the replication journal, client to primary. Stores: the
controllers' metadata log; worker state in RAM only.

**Faults.** Primary pause past its grant expiry. Controller partition from
the primary. Lost renewal. Clock drift within the 10 ms bound. Controller
leadership change. Worker restart with a new incarnation. Out of scope:
clock readings outside the admitted interval, since the plan pauses
service on excessive uncertainty.

**Bounds.** 3 controllers, 2 workers, 2 clients, 1 slot, 1 pause, 1
partition, 1 leadership change. Time in 1 ms ticks with a 3 s lease scaled
down to keep the drift-to-lease ratio. Exhaustive within 15 min.

| Guarantee | Property | Mutant that must break it |
| --- | --- | --- |
| CACHE-G06 | `G06_SinglePrimary`: at every true time, at most one worker passes the authorization check for a slot | `no_uncertainty`: the check compares the clock reading, not its upper bound, with expiry |
| CACHE-G06 | `G06_SuccessorWaits`: a successor's grant starts at or after the previous grant's latest committed expiry | `early_promotion`: the controller grants the successor when the primary stops renewing |
| CACHE-G07 | `G07_OneOwner`: every committed controller state assigns each slot to exactly one group | `move_without_epoch`: a slot move commits without bumping the slot epoch |
| CACHE-G05 | `G05_NoResurrection`: a stale or delayed update never brings back an overwritten value | `accept_retired_epoch`: a replica applies effects from a retired epoch |
| CACHE-G06 | `G06_Recovery`: after the partition heals with a controller leader, each slot eventually has a primary, under weak fairness on renewal and grant | `lost_deadline_on_failover`: a new controller leader issues no grant until a restart |

**Cost.** Per slot group: renewals per second and the bytes per grant
batch. Commands add no controller work, so the per-command count is 0
controller RPCs and 0 fsyncs.

**Rollout.** A lease-duration change while old and new grants coexist.
`G06_SinglePrimary` holds across it.

**Not modelled.** CACHE-G01, byte-for-byte replies: compatibility, checked
by the Valkey differential. CACHE-G04 and CACHE-G10: timing.

## A plan that needs none

```text
Design review: not required. The plan changes the TypeScript ORM's error
messages only. No topology, protocol or L2/L3 module changes.
```
