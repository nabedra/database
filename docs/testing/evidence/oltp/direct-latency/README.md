# Direct point path: where the latency goes (PERF1)

Stage timings and a CPU profile of a direct `table.update` and `table.get` at RF3. Three fixes are
compared with lead `1ea4622`. Measured on 2026-11-21 on the NS11 controlled host and on GitHub runners.
The verdict comes first; the tables and method follow.

## Verdict

- On the controlled host, a serial update was dominated by its commit timestamp: 22.1 ms of the
  leader's 34.6 ms. Three fixes bring the serial p50 from 45.8–52.3 ms to 36.4–40.8 ms. The timestamp
  leg drops to 9.3 ms.
- The sweeps at rate do not move beyond noise. The fixes target work that is paid at most once per
  5 s per worker, or once per queued allocation.
- The direct-path targets still miss. Serial update p50 is about 37–41 ms against 7.6 ms for ScyllaDB.
  Get p50 is about 11 ms against 6.6 ms.
- On a GitHub runner the same build serves a serial update in 7.5–8.1 ms at the client. The
  controlled host costs about 10x more per thread handoff: a native-executor hop takes 0.3–0.5 ms
  there, against 0.03–0.04 ms on the runner. Most of the remaining gap is handoffs, not CPU work.

PERF2c follow-up (HTTP keep-alive on the OLTP API, idle Control reads): [perf2c/README.md](perf2c/README.md).

Final comparative benchmark of the follow-up PR (BENCH2: G1–G7 and the direct-path targets): [final/README.md](final/README.md).

RD1 (hardening): lease reads without the RaftCore round trip, no Timestamp store read, no pre-latch epochs read; per-step tables on the controlled host (external SSD) and runner: [rd1/README.md](rd1/README.md).

## Fixes

| Commit | Change | Effect (controlled host, serial) |
| --- | --- | --- |
| `e0e88b7` | `AllocateDirect` routes with a cached member list that a strong Control read confirmed within 5 s; a failed round reads again | Removes one Control group read per allocation while allocations come faster than every 5 s. Per-update Control ReadIndex on the Control leader drops by about 20/s at 20 updates/s on CI. A serial client (one update per 5 s or more) still pays it. |
| `301fd00` | The Timestamp leader's `AllocateDirect` barrier is the lease read that plan section 5 specifies, not a forced ReadIndex | `ts.allocate.barrier` 1.48 → 0.38 ms |
| `951ab8c` | After an apply discards the direct window (a queued allocation moved the watermark), a background task reserves the next one if this leader served `AllocateDirect` within 30 s | `ts.allocate.refill` 11.8 ms per update → 0. The serial benchmark's queued snapshot check discarded the window before every update. |

Safety is unchanged: quorum, fsync before ack and exactly-once are untouched. The window install rule and the barrier's
applied-leader check are the same; the prefill hands out no values.

## Serial update, controlled host (means per update, ms)

| Stage | `4a57aba` (lead + counters) | `951ab8c` | `817c191` (final) |
| --- | ---: | ---: | ---: |
| Client p50 (median of 10) | 47.2 | 40.9 | 37.6 |
| API `http.dispatch.total` | 38.8 | 29.8 | 36.4* |
| Gateway `gw.direct.send` | 38.6 | 29.6 | 36.1* |
| Leader `leader.mutation.total` | 34.6 | 25.0 | 31.5* |
| — epochs + barrier + dedup + latest (4 local reads) | 1.6 | 1.8 | 1.6 |
| — `leader.attempt.timestamp` | 22.1 | 9.4 | 9.3 |
| —— Timestamp leader `ts.allocate.total` | 13.3 | 0.38 | 0.41 |
| ——— of which ReserveWindow refill | 11.8 | 0 | 0 |
| —— `rpc.call.TimestampCall` (transport + reply header) | 18.1 | 5.3 | 5.3 |
| — `leader.attempt.propose` (Data `client_write`) | 10.4 | 13.2 | 20.1* |
| —— leader append (fsync inside) | 1.5 | 2.6 | 4.6* |
| —— AppendEntries round trip | 3.1 | 3.8 | 8.1* |
| —— leader apply | 2.2 | 2.4 | 3.1 |

\* `817c191` had one slow repetition (131 ms bucket), so its means include it. The client p50 is a median.

The same build on a GitHub runner (4 vCPU, `ubuntu-24.04`, same caps, frame-pointer build for the profile) gives:

| | `4a57aba` | `951ab8c` | `817c191` |
| --- | ---: | ---: | ---: |
| Serial client p50 | 9.2 | 8.1 | 7.9 |
| Leader total | 5.1 | 3.7 | 4.3 |
| Timestamp leg | 2.98 | 1.34 | 1.60 |
| 20/s (`4a57aba`, `951ab8c`) or 50/s (`817c191`): timestamp leg / propose | 1.39 / 1.93 | 0.69 / 2.17 | 0.58 / 1.91 |

## Comparative runs (controlled host, NS11 caps, one system at a time)

Each cell gives acknowledged/offered requests, the median acknowledged rate, and the medians of p50 and
p99 in ms. `base` is lead `1ea4622`, using the NS11 binary `nabedra-db-c74c2d4`; its OLTP sources are
identical to `1ea4622`'s. `final` is `817c191`. Host load ranged from 2 to 6.6 during the runs (see
`load.log`).

| Workload | base (run 1) | base (run 2) | final (run 1) | final (run 2) |
| --- | --- | --- | --- | --- |
| Serial 0.2/s | p50 52.3 | p50 45.8 | p50 36.4 | p50 40.8 (`951ab8c`) |
| Update 1/s | 37.3 / 43.0 | 36.7 / 45.9 | 71.1 / 81.2 (slow host window) | 36.0 / 41.4 |
| Update 5/s | 29.8 / 48.5 | 36.3 / 58.4 | 27.5 / 38.3 | 34.2 / 84.8 |
| Update 20/s | 34.7 / 113 (stops) | 32.2 / 63.0 | 31.2 / 71.9 | 29.3 / 56.5 |
| Update 50/s | – | 45.1 / 161 (stops) | 56.8 / 387, rej 9 (stops) | 31.2 / 83.5 |
| Update 100/s | – | – | – | 64/s acked, rej 477 (stops; driver-bound) |
| Get 1/s | 17.3 / 20.7 | | 10.9 / 14.6 | 12.4 / 16.9 (`951ab8c`) |
| Get 50/s | 11.2 / 34.5 | | 11.3 / 33.2 | 11.1 / 23.1 (`951ab8c`) |

All runs verified their snapshots (`correctness_verified: true`) with 0 unresolved outcomes. The
update sweep's stop rule (p99 above 100 ms or any rejection) fires on host noise. Run-to-run spread on
this host is at least ±6 ms on the p50 at low rates. Gets do not use the timestamp path, so their
change is not attributed to these fixes.

## Where a get goes (1–2/s, `817c191`)

Client p50 10.9 ms. API `http.dispatch.total` 4.1 ms. Leader `leader.get.total` 1.7 ms: lease barrier
0.43, epochs 0.43, latest read 0.42. So about 6 ms lies between the Python HTTPS client and the API's
`dispatch()`, and 2.4 ms between `dispatch()` and the leader.

## Background load on the Control leader

The worker that leads Control and Catalog (`w1`) uses about 0.45 of its 1 CPU at 1–2 updates/s on the
controlled host; the followers use 0.13. It is CFS-throttled in 1–8 of every 56 periods, which is when
p99 jumps above 100 ms. Its stage counters show about 28 Control ReadIndex barriers per second at idle.
Each is a heartbeat round to two followers (about 63 heartbeats/s), and about 70 native-executor hops/s.
Every process's `ControlClient` reads total only about 5/s (`control.read.*`), so most of these
barriers come from another caller inside `w1`. That caller is not yet identified.

## Method

- Stage histograms: `stage_timing` in each worker's `/v1/runtime/stats` and `api_stage_timing` in the
  API's (power-of-two µs buckets; `server/src/oltp_runtime/stage_timing.rs`). The driver stores the
  delta per phase (`stage_summary.direct.<process>.stage_timing`).
  `python3 stages.py runs/<run>.json [stage prefix...]` prints them per operation.
  `python3 summary.py name=runs/<run>.json ...` prints the cells above.
- Controlled host: Docker Desktop 28.3.3, 8 vCPU, 8.22 GB, linuxkit 6.10.14, image
  `nabedra-oltp-perf-builder:1.94.1` (`sha256:1547483c…`), caps verified as in NS11 (workers 1 CPU and
  2.15 GB, API 0.35 CPU, driver 0.65 CPU). `run.sh <label> <binary> serial|update|get`, with the NS11
  workload arguments and `--seed 20261118 --timeout 10`.
- CI profile: the opt-in `perf` job (`[ci perf]`) builds a frame-pointer release binary in
  `rust:1.94.1-bookworm`. It runs the serial pair and 50 updates/s on the runner with
  `python:3.11-slim-bookworm` + `TOKIO_WORKER_THREADS=2`, and samples every worker with `perf record -F 999 --call-graph fp`.
  The `perf-profile` artifact holds the folded stacks.
- Binaries: `4a57aba` `6d3d8268…`, `951ab8c` `93ae3ddd…`, `817c191` `e33feb61…` (bench job, run
  44845977650), base `c74c2d4` `bf370a7e…`.
