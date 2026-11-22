# OLAP streaming guarantees

Module: olap-stream
Level: L2

Same-run recovery from a replicated checkpoint is L2. The other streaming
guarantees are L1. Use a serial scan of the pinned snapshot as the oracle.
After deduplication by sequence, delivered batches must concatenate to that
scan in order. The same reconnect vectors run through both ORM clients.
PyTorch acceptance is an optional real-environment check.

The ST IDs refer to the existing acceptance cases. ST04 covers several replay
and recovery guarantees. ST05 and ST06 check scan and resource behavior.
See the [streaming plan](../../../plans/olap-streaming.md) and
[process evidence](../evidence/olap-streaming/README.md).

| ID | Guarantee | Failure model | Scenarios | Checks | n/a |
| --- | --- | --- | --- | --- | --- |
| OLAP-STR-G01 | A pinned snapshot yields batches of exactly `batch_rows` rows in primary-key order. Only the final batch may be shorter, and `drop_last` can omit it. | Sparse predicates, physical partition boundaries, and concurrent commits or table replacement can affect an in-progress scan. | ST01, ST02, ST05 | U S H | M: The serial scan gives the oracle; F: This row uses valid queries and encodings; D: The scan comparison is part of the unit and history checks; R: A pinned store and simulated concurrent commits exercise the rule; P: Batch shape and order are correctness assertions; K: Finite scans cover the batch boundaries |
| OLAP-STR-G02 | The same UUID and body within a run replay the original handle. | An open reply can be lost. Two APIs can race to open the same identity, including with a conflicting body. | ST04 | U S H R | M: Injected faults exercise the running replay protocol; F: The replay cases use structured bodies; D: The first handle is the expected result; P: Handle identity has no performance target; K: Targeted open retries cover the race |
| OLAP-STR-G03 | Demanding the current sequence again replays its batch or EOF. Demanding the next sequence acknowledges the prior one. | A consumer can pause or disconnect. A reply can be lost or truncated, and a stale or duplicate demand can arrive. | ST03, ST04 | U S H R | M: Generated and simulated sequences exercise delivery and ack transitions; F: These checks focus on the delivery state after known faults; D: The sequence and acknowledged cursor give expected outcomes; P: Sequence correctness has no performance target; K: Directed reconnect and delivery cases cover the boundaries |
| OLAP-STR-G04 | A stream recovers within the same run from its replicated checkpoint. | A coordinator can crash between demand and ack, before or after checkpoint replication. Another worker retains a complete checkpoint. | ST04 | U S H R | M: Simulation exercises the implemented checkpoint protocol at failure cuts; F: Recovery starts from a valid stream; D: The pinned scan and checkpoint give the oracle; P: This row covers recovery correctness; K: Targeted coordinator kills exercise the checkpoint boundaries |
| OLAP-STR-G05 | A cold start abandons old stream handles. | Every worker can stop while a stream or an unanswered open is outstanding. The cluster restarts on the same committed data. | ST04 | S H R | M: The cold-start workload checks the original and replacement runs; U: The assertion depends on complete worker shutdown; F: The fault is loss of in-memory handles; D: The new run and old handle give the expected rejection; P: Abandonment has no performance target; K: A single complete stop exercises the run boundary |
| OLAP-STR-G06 | A live or resumed stream never silently starts the scan over. Recovery may rebuild the current batch from its pinned checkpoint. | Reconnects, coordinator failure, selective filters, and storage jitter can expose a lost cursor or redundant scan. | ST04, ST05, ST06 | U S R P | M: The checks measure work on the actual scan path; H: Scanned and delivered row counts detect repeated work directly; F: The workload uses valid streams and cursor state; D: Cursor and I/O counters give the expected bounds; P: hardware-deferred (absolute streaming latency targets on dedicated hardware); K: Planned read and reconnect trials cover the scan boundaries |
