# Representative competitors by workload

Pick systems that can do the same user-visible work. Start with the closest
match in the relevant row. Add the other baselines when the workload calls
for them. Run the same correctness checks for every baseline.

| Nabedra engine or capability | Representative competitors | Benchmark workload and comparison boundary |
| --- | --- | --- |
| OLTP, replicated point operations | **ScyllaDB**, Apache Cassandra, MongoDB | Point reads and updates, bounded partition scans, and read/write mixes at uniform and skewed loads. Match replicas, consistency levels or concerns, commit durability, and indexes. |
| OLTP, relational transactions | **PostgreSQL**, **MySQL (InnoDB)**, CockroachDB or YugabyteDB for replicated SQL | Primary-key reads, indexed range reads, and read/modify/write transactions. Match transaction scope and isolation, acknowledged commits, and replication. Run only operations expressible with the requested guarantees. |
| OLTP, embedded reference | **SQLite** | In-process keyed reads and durable single-file writes with declared journal and sync settings. Report embedded timing in its own workload with the process and network boundary visible. |
| OLAP, distributed SQL and Parquet | **Apache Spark SQL via PySpark**, ClickHouse, Trino | Projection and filtering, scans, group-by, joins, and batch materialization. Match source snapshot, file layout, scanned columns, selectivity, result materialization, and worker resources. Time ingestion separately when a system loads into its own format. [Spark Parquet documentation](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) |
| OLAP, single-node analytics | **DuckDB**, single-node ClickHouse | Use the same Parquet input for scan, aggregate, and join workloads. Record local versus object-store reads and worker count. For streaming, compare time to first batch, inter-batch wait, and rows/s using the same consumer pace and output shape. |
| Cache, in-memory key/value | **Valkey**, **Redis**, **Memcached** | Warm GET hits and misses, SET with TTL, mixed traffic, and eviction pressure with matched key/value sizes, memory, TTL, and offered load. Use the same RESP path for Valkey and Redis where supported. Use Memcached's native protocol for equivalent operations and record the client and protocol difference. |
| Events, retained partitioned log | **Apache Kafka**, Redpanda, Apache Pulsar; NATS JetStream for workloads with matching semantics | Publish ACK, publish-to-consume latency, consumer lag and catch-up, throughput, and recovery. Match replication, persistence at ACK, batching, partition order, redelivery, consumer checkpoints, and retention. Record Kafka's `acks`, `min.insync.replicas`, and flush policy explicitly. [Kafka topic configuration](https://kafka.apache.org/43/configuration/topic-configs/) |
| Vector search, where supported by the tested revision | **pgvector on PostgreSQL**, Qdrant, Milvus, **Pinecone** | Measure ingest-to-searchable time, filtered and unfiltered top-k latency, and queries/s at the same recall@k against exact ground truth. Match vectors, distance metric, metadata filters, k, and search quality. For Pinecone, record managed tier, region, client placement, and cost when available. Report it as a managed end-to-end workload with network time included. |

## Reuse the repository's runners

- **OLTP.** [Common workload](../../../../scripts/benchmark_oltp_workload.py)
  with the [Nabedra driver](../../../../scripts/benchmark_oltp_nabedra.py)
  and [ScyllaDB driver](../../../../scripts/benchmark_scylla_oltp.py).
  Check their topology and ACK settings before using the results for a claim.
- **OLAP.** [Streaming fixture](../../../../scripts/benchmark_olap_streaming.py)
  and [sample evidence](../../../../docs/testing/evidence/olap-streaming/benchmark.md)
  for first-batch and steady-state wait. Use an equivalent query and result
  consumer on the competitor.
- **Cache.** [Paired Cache benchmark](../../../../docs/testing/cache-benchmark.md)
  documents the Valkey runner, data budgets, load sweep, and standalone
  and cluster profiles.
- **Events.** [Events load acceptance](../../../../server/events/tests/README.md)
  and the [publish performance spec](../../../../docs/testing/perf/events/publish.toml)
  supply the Nabedra workload and metrics. Match the competitor's producer
  and consumer schedules to the engine behavior under review.