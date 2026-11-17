# OLAP stream depth benchmark

ST06 compared depths 0, 1, and 2 for two ranks reading one pinned dense snapshot. Each depth ran one warm-up and three measured repetitions. Every trial produced the exact ordered rows for both disjoint shards. The dense profile also matched the ordinary collect result. Sparse ordinary-request progress, GPU idle time, GPU copy time, multi-GPU jitter, and exact cache hits remain unmeasured in this benchmark.

The dense run used OLAP build `b30fc1ff4527b5c83a13b80e9b1e228c89d7d397` with SHA-256 `6ed815f988132840877f55bc9a31f9c2c6fd99d3bb81d8139c1065fed369e4d8` and fixture archive `cc39ef9240b2e1a9da8b33ecc3d019e638c762af`. The container used 1.5 CPUs and 1,536 MiB of memory with swap disabled and seed 92106. Three real workers advertised 1, 2, and 1 compute threads. The API and workers used loopback TCP and a local conditional object store. The 8-CPU Docker VM also ran other workloads. The host load just before launch was 56.29 over one minute.

| Dense depth | First batch p50/p95/p99, ms | Steady next p50/p95/p99, ms | Rows/s, median trial | Encoded bytes read/row, median trial |
| ---: | ---: | ---: | ---: | ---: |
| 0 | 2,547/3,049/3,049 | 807/2,533/2,533 | 74.9 | 90.0 |
| 1 | 2,259/3,446/3,446 | 897/1,352/1,352 | 77.8 | 121.1 |
| 2 | 3,653/4,113/4,113 | 1,186/3,746/3,746 | 62.5 | 132.9 |

![Dense p95 steady next wait in seconds by prefetch depth](benchmark-next-wait.png)

The dense profile read 512 rows with 32–80 character payloads in 64-row batches. Through its last sample, container memory peaked at 214.4 MiB with zero OOM events. The highest sampled worker RSS was 45.4 MiB, and the highest sampled charged working memory was 132.9 MiB. The trial records retain rank skew, CPU time, Parquet GET bytes, worker proxy timing, and sampled slot occupancy. The B01 observations cover sampled utilization. An exact idle-slot bound remains unmeasured here. [Trial records](benchmark-results.json)

Quantiles use the lower order statistic over pooled rank samples. First-batch quantiles have six measured samples per depth, and steady-next quantiles have 18. These sample sizes can produce identical p95 and p99 values.

## Run boundaries

- The first attempt hit a 15-second SDK deadline during a 1,024-row setup mutation. It recorded zero trials and finished in 33.43 seconds.
- Chunked setup allowed the 1,024-row retry to finish one dense warm-up and one measured trial per depth. The run stopped after those six verified trials. The partial results retain their original fixture pin and are outside the 512-row summary.
- The smaller combined run hit its 300-second watchdog after completing all 12 dense trials. The sparse profile recorded zero trials.
- The separate sparse-only attempt seeded all 640 rows. Its serial collect setup check hit the 40-second SDK deadline. The process exited after 156.95 seconds, with a 217.4 MiB memory peak and zero OOM events. The bounded fixture repair uses the accepted setup rows as the sparse oracle for every timed batch.

## Reproduce in Docker

Use a Linux x86-64 container with Python 3.11, procps, the pinned binary, and the fixture archive. Set the container CPU quota to 1.5 and both its memory and memory-plus-swap limits to 1,536 MiB. Run the dense profile separately on a host with room under those limits.

```sh
export PYTHONPATH=orm/python/src OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
timeout -k 8s 300s python3.11 scripts/benchmark_olap_streaming.py \
  --binary "$OLAP_BINARY" --commit b30fc1ff4527b5c83a13b80e9b1e228c89d7d397 \
  --fixture-commit cc39ef9240b2e1a9da8b33ecc3d019e638c762af \
  --profile dense_fast --evidence dense.json
```

[Benchmark fixture](../../../../scripts/benchmark_olap_streaming.py) · [Scheduler contract](../../scheduler-contract.md) · [Streaming plan](../../../../plans/olap-streaming.md)
