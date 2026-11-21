# PERF1 (hardening): API CPU limit vs compaction under sustained writes

Branch `nabedra/hd-perf1`. Every number here is from GitHub-hosted runners (ubuntu-24.04, 4 vCPU), so treat them as relative
only. The controlled-host window was abandoned: other threads' local builds pushed the 1-minute load to 7–25 and Docker
returned a 500 (see `host-load.log`). The one controlled-host run that finished (`host-fac-before-api0.35-contended.json`)
reproduces the mechanism, but its latencies are not comparable.

- **Before:** BENCH2's candidate binary `5eb80fb` (artifact of run 44878990620). Its product code equals the base, 7e110cb.
- **After:** this branch's release build (`perf1-bin` job).
- **Fixed in both:** same driver (`scripts/benchmark_oltp_nabedra.py`), seed 20261118, NS11 worker and driver caps
  (1 CPU / 2.15 GB per worker, driver 0.65 CPU), `TOKIO_WORKER_THREADS=2`, direct path on.

## Limits, causes and fixes

| Limit (BENCH2) | Measured cause | Evidence | Fix |
| --- | --- | --- | --- |
| ~100 updates/s acknowledged at 200 offered; 429 `executor admission capacity exhausted` | The API admitted direct requests on the queued lane's `Executor::new(2, 16)`, and each one held one of its 2 workers for the whole leader round trip. Throughput was capped at 2 / route time. API CPU was not the limit. | BENCH2 raw stages at 200/s: `http.dispatch.spawn_wait` 135 ms against `gw.route` 19 ms. The API used 0.8 ms of CPU per update (0.10 CPU at 100/s). On the runner, API caps of 0.35, 0.7 and 1.0 and no cap give the same results (`ci-api-factor.json`). | `HttpRuntime::direct_ingress` (64 slots). A direct request that the gateway routes queued also takes a queued-lane slot. `DirectLink::send` waits for the leader's lane slot instead of refusing at once. |
| Soak refusals of 4.1% at 50/s and 9.8% at 100/s; p99 up to 2.4 s | **Not compaction.** Every ~5,000 Data entries (openraft's default `SnapshotPolicy::LogsSinceLast(5000)`), each replica exports a full group snapshot. `HostedState::builder()` runs the whole export on the state-machine task while holding the group's application-order lock, so applies stop and commits stall for 1.4–3.6 s. The 2-worker API gate turned each stall into 429s. | Stall windows every 52 s at 100/s and every 101 s at 50/s, the same with compaction held off (`ci-soak-stall-windows.json`). With compaction held off, w1 writes bursts of 140–480 MB in exactly those windows (cgroup `io.stat`). Holding compaction off (L0 peak 30–37, 0 compaction bytes) changes neither the stalls nor the acknowledged rate. `write_delays` = 0 and `stall_ms` = 0 in every soak. | Not fixed here; proposal in the PERF1 report. The API fix only turns the 429s into waiting. |

## Before → after (runner)

The API-factor rows are 3 × 5 s per rate. The soak rows are 5 × 60 s on 80,000 × 2 KiB rows. `cap` counts arrivals the driver
refused because all 32 of its clients were busy. For the soaks, p99 is the median of the five phase p99s, with the largest
phase p99 in brackets.

| Run | Offered/s | Before: ack, 429, p50/p99 ms | After: ack, 429, cap, p50/p99 ms |
| --- | ---: | --- | --- |
| API 0.35 | 100 | 1500/1500, 0, 3.0/18.1 | 1500/1500, 0, 0, 2.8/5.4 |
| API 0.35 | 200 | 2993/3000, 7, 3.3/23.7 | 3000/3000, 0, 0, 3.1/7.8 |
| API 0.35 | 400 | 5474/6000, 388, 4.1/41.8 | 5714/6000, 0, 286, 3.9/229 |
| API none | 400 | 5550/6000, 444, 4.0/50.5 | 5693/6000, 0, 307, 4.0/198 |
| Soak 100/s, API 0.35 | 100 | 28970/30000, 1030, 2.2/24.3 [56] | 29089/30000, 0, 911, 2.2/23.7 [48] |
| Soak 50/s, API 0.35 | 50 | 14738/15000, 262, 2.2/4.2 [18] | 14786/15000, 0, 214, 2.3/1542 [1624] |
| Soak 100/s, API none, compaction held | 100 | n/a | 28900/30000, 0, 1100, 2.3/4.7 [76] |

After the fix, the API refuses nothing. At 400/s the remaining bound is the leader's ApiIngress lane: 4 handler slots per
peer, with an average `gw.direct.lane_wait` of 6.4 ms. Requests now queue there instead of being refused, which raises p99.
In the soaks, requests now wait out the snapshot stalls instead of failing. So a 50/s phase that contains a stall has a
p99 of about 1.5 s; before, that stall produced 429s.

The final head (`3479410`, after the lane-headroom fix) repeated the API sweep in run 44907326240
(`ci-api-factor-final.json`). That runner was noisier: p99 was 60–450 ms even before the fix at 100/s. Before the fix, the
API refused 0.3–2.7% at 100/s, 1–15% at 200/s and 7–32% at 400/s across the four API caps. After it, the API refused
nothing at any cap; the only refusals left were the driver's 32-client cap during stalls. The snapshot-export stall also
falls inside these short runs, because together they write more than 5,000 entries.

G1: every run has `correctness_verified: true`, 0 mismatches (5,000 rows in the factor runs; 500 sampled keys in the soaks),
0 unresolved and 0 failed.

## Per-update CPU (runner, 150 updates/s, frame-pointer build; `stacks.py`, `ci-stacks-150.txt`)

Run 44904328496 (`c813dae`) is the first run in which perf sampled the API's server process; earlier runs sampled the CLI wrapper.

| Stage | API (0.34 ms per update) | Leader w1 (2.61 ms per update) |
| --- | ---: | ---: |
| Tokio scheduling and wakeups (user + kernel futex/epoll) | 39% | 33% |
| HTTP (hyper, axum, dispatch) | 20% | – |
| Gateway RPC / transport | 10% | 6% |
| TLS | 6% | 3% |
| JSON | 6% | 6% |
| Routing / topology cache | 3% | 0% |
| Raft | – | 10% |
| nabedra-kv | – | 12% |
| Native-executor hops | 2% | 9% |
| Leader execution (direct / MVCC) | 1% | 5% |
| Allocator, other kernel, other | 14% | 16% |

On the runner the API's CPU per update is about an eighth of the leader's. On the controlled host BENCH2 measured 0.8 ms
(API) and 5.7 ms (leader) per update. The leader's CPU is the next throughput limit, and batching (GC1) is the lever for it.

## Files

- `run.sh`: controlled-host runner. It waits for the 1-minute load to drop below 3. The CI equivalent is the `perf1-factors`
  job in `.github/workflows/nabedra-build.yml`, opted in with `[ci perf1]`.
- `analyze.py`: per-phase summary.
- `stacks.py`: perf folded-stack buckets.
- `ci-*.json`, `ci-*-runs.txt`, `ci-soak-stall-windows.json`: compact outputs.
- The full outputs are in the hardening ledger, under `perf1/ci/`.
