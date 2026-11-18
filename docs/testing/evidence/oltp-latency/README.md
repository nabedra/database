# Replicated OLTP point-update benchmark

Three independent Linux Docker containers hosted the Nabedra workers. Each worker had a separate named volume on the same Docker Desktop VM storage device. A fourth container ran the production remote HTTPS API, and a fifth ran the load driver. Docker inspection verified an aggregate limit of 4 CPUs and 7.9 GB, with swap disabled and no OOM kills. Each worker had 1 CPU and 2.15 GB, the API had 0.35 CPU and 550 MB, and the driver had 0.65 CPU and 900 MB.

The fixture seeded 2,000 rows with an integer primary key and integer value. It verified three voters for the table's initial Data group. Each phase chose distinct existing keys uniformly without replacement, with the same seeded arrival schedule across systems. Each rate had a three-second warm-up and three five-second measured repetitions. Clients used persistent connections and made at most 32 updates concurrently. The fixture measured from scheduled arrival through the response and checked every final key and value, including the expected results of late receipts. Goodput counts acknowledged writes and includes drain time. Percentiles describe successful responses. Rejections, client capacity limits, terminal failures, and unresolved identities have separate counts.

The 2021 reference chart used three AWS i3.4xlarge database instances totaling 48 vCPUs, additional load generators, a disk-intensive 1 TB dataset per node, and 30 minutes per offered rate. The local test uses a small warm dataset and the application's HTTPS gateway. The ScyllaDB reference uses prepared native CQL. It measures the overhead of both products' normal request paths. [Published ScyllaDB benchmark](https://www.scylladb.com/2021/08/24/scylladb-vs-cassandra-performance-results/)

## Nabedra baseline

Commit `2e26418` built with Rust 1.94.1, the release profile, the `oltp` feature alone, and the verified RocksDB 11.8.1 archive previously used for the engine integration build. The Rust server binary SHA-256 was `af7752e3c8f54f4560ae93895d5f7e5cf72ef6a153e2842ee4e9f5550c243104`. The archive SHA-256 was `8723f6ffe373a3646de577a711c42ff0dcea433a83837db2d03b7b20c506a8f8`. [RocksDB archive provenance](../olap-streaming/README.md)

| Offered updates/s | Median successful updates/s | Writes acknowledged across 3 repeats | Median successful p99 |
| ---: | ---: | ---: | ---: |
| 1 | 0.600 | 9 / 15 | 2,513 ms |
| 2 | 0.687 | 15 / 30 | 3,307 ms |
| 5 | 0.781 | 18 / 75 | 4,089 ms |
| 20 | 0.749 | 18 / 300 | 4,549 ms |
| 50 | 0.727 | 17 / 750 | 5,018 ms |
| 100 | 0.701 | 18 / 1,500 | 5,283 ms |
| 200 | 0.601 | 17 / 3,000 | 6,551 ms |

Across the 21 measured phases, 112 updates succeeded, 3,802 received explicit rejection and 1,756 hit the driver's concurrency cap. The snapshot oracle found zero mismatches and the driver observed zero unresolved writes. The worker's first-dispatch count matched all 112 acknowledged writes. The gateway reported that most rejected requests had exhausted parsing memory. The API container's measured cgroup memory peak was 46.6 MB. Its logical scheduler charges an 88 MiB staging reservation while each HTTP request waits for remote submission. This accounts for the parsing-memory rejections even at low process memory use.

This is a local O23/O24 and B02 workload subset. The fixture checks ordinary replicated writes, replies, snapshot contents, and bounded resource use. Recovery barriers, wider workloads, physical availability zones, and the full compatibility suite have separate assertions. [OLTP contract](../../oltp-contract.md) · [Scheduler contract](../../scheduler-contract.md)

## Iteration 1 · TCP_NODELAY

This build enabled TCP_NODELAY on both ends of the worker mTLS connection. Under the same workload and limits, 110 of 5,670 measured writes succeeded. Peak median goodput was 0.706 successful updates/s. At 5 offered updates/s, median successful p99 rose from 4,089 to 4,740 ms. Every expected snapshot matched and the driver observed zero unresolved writes. This change was removed.

A separate serialized diagnostic with RPC trace logging isolated two successful writes on the same binary. Their client service times were 1,724 ms and 1,743 ms. Each produced 42 paired service calls, including nested calls. The union of their logged route, transport and decode intervals covered 1,363 ms and 1,379 ms. Peer transport covered 1,107 ms and 1,132 ms, including work at the remote endpoint. Route lookups covered 315 ms and 306 ms. The trace measures grouped call durations. It leaves TLS, quorum, disk flush and native application time combined within those calls.

## Iteration 2 · HTTP parsing reservation

The ingress build reduced the parser staging charge to the owned request buffer size after validation. At 1 offered update/s it acknowledged 15 of 15 measured writes, with median goodput 0.68/s and median successful p99 4,902 ms. The baseline acknowledged 9 of 15 at this rate. The higher admitted concurrency exposed local outgoing RPC lane exhaustion during accepted work. The 2/s phases reported uncertain outcomes in the manifest, timestamp and commit paths. The candidate remains unqualified for general use pending a bounded progress fix.

A write reported as unknown in one phase took effect after that phase's snapshot. The next snapshot saw its value and the initial oracle treated it as a mismatch. The driver must account for uncertain outcomes across phase boundaries. The late value alone does not establish an incorrect database write. The Ed25519 probe on this same binary reached normal HTTPS and worker mTLS readiness, then hit the same fixture issue before it provided enough complete repetitions for a crypto comparison.

## Iteration 3 · TLS certificate algorithm

The unchanged baseline binary ran against an Ed25519 CA and Ed25519 worker and API certificates. Both runs verified RF3, the same workload and resource limits, and every final row. Each rate used three measured repetitions.

| Offered updates/s | RSA median p99 | Ed25519 median p99 | RSA median successful updates/s | Ed25519 median successful updates/s |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 2,513 ms | 2,596 ms | 0.600 | 0.528 |
| 5 | 4,089 ms | 4,212 ms | 0.781 | 0.736 |
| 20 | 4,549 ms | 4,637 ms | 0.749 | 0.764 |

Changing the certificate algorithm gave no consistent performance gain. One Ed25519 write returned an ambiguous response during the timed window. A subsequent receipt confirmed that it succeeded. The successful-throughput counts retain the original synchronous outcome.

## ScyllaDB reference

The ScyllaDB 6.2.3 reference ran three nodes and the driver with a combined cap of 3.65 CPUs and 7.35 GB. Each node and the driver had the same limits as their Nabedra counterparts. This topology serves prepared CQL directly. It used RF3, QUORUM and batch commit-log sync. The fixture verified the effective sync mode on every node. On Docker Desktop the Linux AIO limit prevented the third node from starting, so the run used the epoll backend. [ScyllaDB Docker Desktop workaround](https://github.com/scylladb/scylladb/issues/16806)

| Offered updates/s | Median successful updates/s | Writes acknowledged across 3 repeats | Median successful p99 |
| ---: | ---: | ---: | ---: |
| 1 | 1.00 | 15 / 15 | 5.45 ms |
| 2 | 2.00 | 30 / 30 | 7.97 ms |
| 5 | 5.00 | 75 / 75 | 8.85 ms |
| 20 | 20.00 | 300 / 300 | 9.20 ms |
| 50 | 50.00 | 750 / 750 | 14.47 ms |
| 100 | 99.96 | 1,500 / 1,500 | 9.91 ms |
| 200 | 200.00 | 3,000 / 3,000 | 71.98 ms |

Every measured write returned successfully and every snapshot matched. At 200 offered updates/s, p99 across the three repetitions ranged from 52.67 to 197.76 ms. This sweep stops at 200 updates/s.

![Median successful update latency and throughput by offered rate](point-updates.png)

## Reproduce

Build or obtain a pinned Nabedra Linux release image. Use an idle Docker host and empty result directories. Run one engine at a time.

```sh
python3.11 scripts/benchmark_oltp_nabedra.py --image nabedra:oltp \
  --output artifacts/oltp-latency/nabedra --rates 1,2,5,20,50,100,200
bash scripts/benchmark_scylla_oltp.sh artifacts/oltp-latency/scylla \
  --rates 1,2,5,20,50,100,200
```
