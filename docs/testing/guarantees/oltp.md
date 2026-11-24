# OLTP guarantees

Module: oltp
Level: L3

Eligible synchronous single-key operations use the direct path by default.
The range leader commits each mutation in one Data log entry, which can also
carry other requests' commits. With `direct_oltp: false`, requests take the
queued path. Queue rules still apply to explicit queued requests and
receipts, schema changes, and cross-range batches. For eligible direct
operations, the direct-path rules take precedence in Q03, Q05, and O23.
See the [OLTP contract](../oltp-contract.md) and the
[direct-path plan](../../../plans/oltp-direct-path.md).

On the direct path, a retry of a mutation with the same request ID and body
returns the first definite result of that identity while its dedup record is
retained. That includes a no-op (`update` or `delete` of a missing row) and a
row conflict (`insert` of a present row). Unknown outcomes, unavailable or
overloaded answers and validation errors are not results and are not recorded.
Reads have no identity. A dedup record is retained for
`direct_dedup_retention_ms`, 24 h by default. Once it is deleted, a retry runs
as a new request. The cost is a Data log entry for outcomes that commit none
today, so no-ops and refused inserts get slower. The rule covers the same body
only and leaves D03 as written for a different body.

Status: the shipped executor recomputes no-ops and row conflicts on retry
today, which is a known gap (DR-1, draft PR #118, reproduced locally on one
seed in a simulation fault model, not independently reviewed), and the
implementation waits for a design review.

O18 and O27 keep their local kind cases. O19 backup and restore still requires
real S3. The direct-path benchmark supplements O24, O25, and NS18 with paired
runs. Its absolute latency targets require dedicated hardware.
See the [acceptance guide](../oltp-acceptance.md) and
[direct-path benchmark](../../../plans/oltp-direct-path.md#benchmark).

| ID | Guarantee | Failure model | Scenarios | Checks | n/a |
| --- | --- | --- | --- | --- | --- |
| OLTP-G01 | An acknowledged write survives any single-AZ failure and a full stop. | One availability zone can fail, or all workers can stop and restart from their disks. Storage faults include torn or reordered unsynced writes, corruption, and sync failure. | O06, O11, D04, D09, NS04, NS05, NS06, NS07, NS08, NS09, NS16 | M U S H R K | F: Decoder faults do not test acknowledged-write durability; D: The oracle is the acknowledged write set and the allowed durable prefix; P: Durability is a correctness assertion |
| OLTP-G02 | Completed operations admit a serial order that respects real time. | Same-key and cross-shard operations race with snapshots, leader changes, delayed replies, and clock jumps. | O08, O09, D06, D08, O07 | M S H D K | U: The serial order spans operations and replicas; F: Structured histories exercise the order; R: Simulation and history checking cover the interleavings for this row; P: Real-time order constrains outcomes rather than elapsed latency |
| OLTP-G03 | Each request identity executes once, cluster-wide on the queued path and per table and key on the direct path. On the direct path, a retry with the same request ID and body returns the first definite result of that identity, including a no-op or row conflict, while its dedup record is retained. | A gateway or coordinator can fail before returning a result. Retries can race, change the body, or cross the direct-path activation boundary in O15. | Q05, O10, O14, D02, D03, O15 | M S H R | U: Deduplication spans retry routes and durable decisions; F: Valid and conflicting retries are covered by protocol checks; D: The receipt and applied-write count give the expected result; P: Identity reuse has no performance target; K: Targeted retry and activation cuts exercise the rule |
| OLTP-G04 | Return `not_applied` only with proof of non-application. Otherwise, report uncertainty as `outcome_unknown`. On the direct path, a row conflict is a definite result of its identity (OLTP-G03); unknown outcomes, unavailable or overloaded answers and validation errors are not results. | A reply can disappear after a durable commit. A session, quorum, or storage path can fail while the decision is ambiguous. | O10, O29, D05, D10, T04 | M S H R | U: Outcome proof spans durable and remote state; F: Ambiguity requires a valid request and an injected fault; D: The durable decision gives the expected result; P: Outcome classification has no performance target; K: Faults are injected at specific decision boundaries |
| OLTP-G05 | Batches are atomic across shards. | Any participant can fail before preparation, during the decision, or before finalizing. A snapshot can race a partially finalized commit. | O07, T01 | M S H | U: Atomic visibility spans participants; F: The workload uses well-formed batches; D: Snapshot visibility follows the decision; R: This row requires modeled and simulated cut histories; P: Atomicity has no performance target; K: Boundary cuts exercise the invariant |
| OLTP-G06 | Routes, topology and schema epochs are never served stale. | A split, leader move, table drop, or index publication can race writes and leave a gateway with stale routing or schema information. | O03, O12, O16, O28, D07, D11 | S H R | M: Simulation exercises the combined route and schema transitions; U: Stale authority spans peers; F: The faults use valid requests addressed to old epochs; D: Current authorities and epochs give the expected outcomes; P: Freshness has no performance target; K: Topology and schema changes are exercised at explicit barriers |
| OLTP-G07 | Accepted queued work stays recoverable while a complete copy survives. A full worker stop abandons outstanding work. | Gateways and workers can fail or partition. Copies can be incomplete, delayed, or lost during handoff. All workers can stop with work pending. | Q01, Q02, Q03, Q04, Q06, O13, O14, O15, O23, R01, R02, R03, R04, R05, O30, O31, O32 | M S R | U: Retention spans worker copies and run fences; H: The checks inspect copies and run state directly; F: The fault is loss of request state; D: Accepted identities and durable decisions give the expected outcomes; P: Retention has no performance target; K: Targeted stop and handoff cases cover the failure boundaries |
| OLTP-G08 | Membership changes never lose a required voter, a queue copy, or a committed entry. | Provisioning can fail or lose its reply. Learners can lag and workers, mounts, or pods can be replaced during growth and drain. | O17, O18, O26, O27, R03 | M S R | U: Safety spans membership, placement, and storage state; H: Membership and copy proofs are inspected directly; F: Lifecycle checks use valid requests; D: The current membership and copy requirements give the expected state; R: cloud-deferred (O18 and O27 EC2, EBS, and EKS lifecycle checks); P: Membership safety has no performance target; K: Faults are placed at membership transitions |
| OLTP-G09 | Resources stay within budgets and each reservation settles exactly once. | Load saturates CPU, memory, and control queues. Work can fail, panic, cancel, or time out while it owns a reservation. | O05, O13, O22, C01, C02, C03, C04, C05, C06, C07, M01, M02, M03, F01, F02, F03 | U S R P | M: The checks inspect the actual resource counters; H: Counters and reservation owners establish settlement; F: The focus is allocation and cancellation after admission; D: The declared limits and baseline give the expected counts; K: Saturation and settlement are tested within the workload |
| OLTP-G10 | Writes stay available with one voter paused or lost. | One voter can pause or fail while a healthy quorum remains. Losing the quorum must prevent success. | D05, O06 | S R | M: The check measures progress in a running quorum; U: Voter loss requires multiple replicas; H: The workload asserts write completion and deadline directly; F: Requests are well formed; D: Quorum membership determines the expected result; P: The deadline is a progress assertion; K: Targeted voter pauses exercise the availability boundary |
| OLTP-G11 | Backups restore exactly to a transaction cut. | All live copies can be lost. Keys, archive pieces, or the final manifest can be unavailable, truncated, or fail to publish. | O19, NS20 | S R | M: The restored state is checked against the committed cut; U: The workflow crosses snapshot export, store, and restore; H: Cut, schema, and range contents are compared directly; F: Archive failures are injected by the backup checks; D: The source transaction cut gives the expected contents; P: This row covers restore correctness; K: A backup and restore drill exercises the cut |
| OLTP-G12 | Both ORMs and both request paths produce identical results. | Topology, path, and client implementation can differ. Inputs can be malformed or use types that expose decoder drift. | O01, D01, U01, U02 | D R F | M: The comparison runs the actual clients and paths; U: This guarantee spans the public client and server boundary; S: Cross-client parity uses the public wire path; H: Results, errors, and types are compared directly; P: Parity concerns behavior; K: A shared program exercises parity without an extended run |
| OLTP-G13 | OLTP meets the latency, throughput and resource targets. | Warm and cold caches, mixed load, hot keys, maintenance, contention, and CPU or storage limits can affect timings. | O24, O25, NS18 | P | M: Timing and usage require measurement; U: The workloads measure the running engine; S: Virtual time cannot establish latency; H: Performance uses resource and latency samples; F: The measured workload is fixed; D: Paired comparisons belong to P; R: The benchmark itself runs on the required environment; P: hardware-deferred (absolute OLTP latency targets, including the direct-path benchmark); K: These cases use bounded benchmark trials |
