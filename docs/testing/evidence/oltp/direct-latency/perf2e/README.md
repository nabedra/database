# PERF2e: serial-pace keep-alive and benchmark verification outside the measured phases

## Changes measured

- **API listener.** The client-facing OLTP listeners keep an idle connection 30 s instead of 5 s. A request head still has 5 s from its first byte. A full listener evicts its oldest idle connection for a new one.
- **ORMs and driver.** Both ORMs (0.2.2) and the benchmark driver reuse a connection idle up to 25 s instead of 2 s.
- **Benchmark verification.** The driver reads one verification snapshot after the last measured phase, instead of a queued snapshot after every phase. `results['verification']` records its wall time and driver CPU.

## Controlled host: serial pair

Workload: `run.sh <label> <binary> serial`, which is `--rates 0.2 --seconds 5 --warmup-seconds 5 --repetitions 10 --clients 1 --rows 2000 --seed 20261118 --timeout 10`, direct_oltp on, NS11 caps.

Runs:
- **base:** binary `nabedra-db-31022f8`, the tree of `734c815` (sha256 `acdcc665…`, the same bytes as PERF2c's `1feeb07`), driven by the `734c815` driver.
- **cand:** binary `nabedra-db-5dc696f` (sha256 `ab2371e4…`), driven by the PERF2e driver.
- **cand, base driver:** the candidate binary driven by the `734c815` driver, which keeps the 2 s reuse rule and the snapshot between phases.

The five runs went back to back in one lane session, 05:11–05:21Z (`load.txt`). The lock was taken at a 1-minute load of 2.58; load then rose to 5–7 during the runs. Every run had `correctness_verified: true`.

Latencies are in ms over the 10 measured updates. CPU is container CPU inside the measured phase windows.

| Run | p50 | p90 | max | client − dispatch | API dispatch | new / reused connections at the API per update | w1 CPU | run wall s | verification s |
| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| base 1 | 43.2 | 63.5 | 63.9 | 9.9 | 36.9 | 2 / 0 | 0.316 | 108 | between phases |
| cand 1 | 32.5 | 42.1 | 57.4 | 3.2 | 31.8 | 1 / 1 | 0.099 | 58 | 4.5 |
| cand, base driver | 41.9 | 50.4 | 56.1 | 10.8 | 31.9 | 2 / 0 | 0.285 | 104 | between phases |
| base 2 | 45.1 | 67.0 | 74.0 | 10.6 | 38.8 | 2 / 0 | 0.297 | 103 | between phases |
| cand 2 | 34.1 | 52.6 | 89.3 | 2.9 | 36.9 | 1 / 1 | 0.095 | 58 | 5.0 |

Where the time went:
- **Connection count.** The API counts two requests per measured phase: the update, and the driver's stats fetch, which always opens a new connection. Per phase, the driver opened 5 connections on the base and 4 on the candidate (`stage_stats_*.driver_cgroup.connections_opened`). So in the candidate the update itself reuses its connection.
- **TLS handshake.** About 7 ms per update leaves the client − dispatch leg (9.9–10.8 → 2.9–3.2 ms). That is the handshake a serial caller paid. The candidate binary with the base driver still pays it, so the gain needs both the server's 30 s bound and the client's 25 s reuse.
- **`w1` CPU in the measured windows.** It drops from 0.29–0.32 to 0.10 once the snapshot check is no longer between phases.
- **Dispatch time.** API dispatch varies 31.8–38.8 ms across the runs, and that difference is not separable from host noise. As in PERF2c, most of it is the Data commit's fsync on this host.

## Runner (`[ci perf]`, run 44868070293, head `5dc696f`)

| | PERF2c on the same base bytes (run 44857409138) | PERF2e head |
| --- | ---: | ---: |
| serial p50 | 9.3 | 5.17 |
| serial client − dispatch | 3.5 | 0.74 |
| 50 updates/s p50 / p99 | 4.6 / 9.3 | 3.4 / 6.9 |
| 50/s reused connections | 99.8% | 99.9% |
| w1 Control ReadIndex at 50/s | 14.3/s | 14.1/s |

The PERF2c column comes from another runner on another day. Runner-to-runner variance is not measured.

## Files

- `analyze.py`: the per-run summary above. Usage: `analyze.py <run dir>...`.
- `serial-controlled-host.jsonl`, `serial-runner.jsonl`: its output.
- `runs/*.json`: compact records (settings, statuses, latency distributions, snapshot reports, verification).
- `load.txt`: the session's lane and load log.

Raw outputs, including samples and cgroup samples, are kept in the ledger (`perf2e/runs/`), not in the repository.
