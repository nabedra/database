Reduce end-to-end OLTP latency and increase sustained throughput under a four-CPU, eight-GB Docker budget.

## Measure the existing cluster

Build the base commit and each candidate with the same optimized Rust profile and RocksDB version. Run a separate production API with three registered Nabedra workers and persistent Docker volumes. Verify RF3, ready membership, and complete terminal outcomes. Keep the load driver within the aggregate four-CPU and 8,000,000,000-byte cap. Run the ScyllaDB RF3 and QUORUM reference on the same host and with the same per-node and driver limits, one database at a time. Record ScyllaDB commit-log sync configuration, client protocol, data layout and layer-specific acknowledgment conditions.

Use persistent client connections and seeded point updates against existing integer keys. Use a warm-up and three measured repetitions at each fixed arrival rate. Measure latency from scheduled arrival and report p50, p90, p95 and p99, acknowledged writes per second, overload, deadline failures, unresolved identities and final values. Capture driver timing and worker CPU, memory, I/O, network and available queue statistics. Compare the last stable common arrival rate and the highest rate each system can sustain with p99 under 10 ms. Sweep further if the highest point still meets that threshold. Record the benchmark commit, binary and image digests, CPU and memory limits, workload shape and run durations. The 2021 ScyllaDB chart used 48 server vCPUs, a 1 TB dataset per node and external load generators. It supplies a latency reference and workload shape. [Published benchmark](https://www.scylladb.com/2021/08/24/scylladb-vs-cassandra-performance-results/)

## Iterate production changes

| Order | Candidate | Check |
| --- | --- | --- |
| 1 | Enable TCP_NODELAY on both ends of the worker RPC connection. The request uses successive small TLS writes. | The paired sweep showed no improvement in peak goodput. Remove this candidate and trace individual writes. |
| 2 | Release the HTTP parser scratch reservation before awaiting the worker submission. Keep the charged body through the handoff. | Retest admission at measured overload points, small receipts during saturation and existing invalid-body outcomes. |
| 3 | Benchmark Ed25519 and RSA TLS material against the same release binary and topology. | Keep socket framing, authentication, node limits and the workload fixed. Compare elapsed time and successful throughput. |
| 4 | Read the catalog descriptor and routing root as a single bounded point page against the pinned snapshot. Retain the catalog epoch check. | Check identical capture digests, missing keys, intents, stale epochs and split routing. Measure the saved catalog ReadIndex. |
| 5 | Send same-process worker RPCs over bounded in-memory streams with the current framing and endpoint checks. | Verify identical caller, cluster, group, incarnation and lane checks. Preserve the worker-owned handler lifetime through cancellation and shutdown. |
| 6 | Try bounded reuse of authenticated peer connections across processes and hosts. | Negotiate support with older peers. Recheck current authorization on each frame, retain progress-lane capacity, and preserve uncertain-outcome semantics after a lost response. |
| 7 | Issue a bounded number of same-phase queue replica RPCs concurrently. Keep all-ready acknowledgment, per-peer version checks and the authority recheck. | Exercise copy barriers, retries and takeover. |
| 8 | Replace fixed gateway observation sleeps with a bounded, authority-checked completion wait. | Compare synchronous latency and check receipt races, result retention and gateway restart. |
| 9 | Profile the remaining transaction cost. Consider an atomic Status command to install and seal a one-page participant manifest. | Check partial legacy drafts, exact retry, durable apply, visibility and fencing. Keep compatibility with multi-page manifests and older replicas. |

After each isolated measurement, keep the candidate only when a repeatable improvement justifies the change. Preserve all-ready in-memory queue replication within each cluster, abandon pending work after full shutdown, retain committed mutations, and keep the published API and ORM compatible. Keep peer connections and routing scoped to the authorized database and cluster.

## Validate

The performance loop exercises a subset of O03, O08, O23–O25 and B01–B02. Report each executed subset and its limits. Post-optimization checks cover O01, O03–O11, O13–O16, O22–O23, O29 and mapped C07, M02, Q01–Q06, R01–R06, F03, T04, U01–U03. Run targeted barrier tests for changes to the queue and observation path. Also run the existing OLTP library and integration suite and production ORM compatibility tests with the same resource cap. Keep provider-specific assertions separate from local Docker evidence. [OLTP contract](../docs/testing/oltp-contract.md) · [Scheduler contract](../docs/testing/scheduler-contract.md)
