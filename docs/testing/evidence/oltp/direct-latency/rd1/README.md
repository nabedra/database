# RD1: coordination removed from direct point reads and the update's read-before-write

## Builds

- **Base:** `5eb80fb`, which has the same OLTP sources as `7e110cb` and `28ca98c`.
- **Candidate:** `f2e2199`.
- **Binaries:** both are `[ci bench]` builds (`nabedra-db-5eb80fb` from BENCH2, `nabedra-db-f2e2199` from run
  44897050330; the candidate's SHA-256 is `2c62a7a3…`).
- **Workload:** NS11 caps through `../run.sh`, with `--seed 20261118 --timeout 10`. The verification snapshot runs after
  the last phase.

## Changes

1. **Lease reads check the lease on the caller's task.** They use openraft `enable_local_lease_read` and no longer make
   a RaftCore round trip. This covers direct gets, the direct leader's reads and Timestamp `AllocateDirect`.
2. **`AllocateDirect` makes no store read.** It takes its applied position from consensus memory.
3. **Direct mutations make no store read of the group's epochs before they latch.** They use the previous snapshot's
   epochs, and the attempt's own snapshot checks them. A changed direct schema restarts the mutation.

## Controlled host (Docker on the external USB SSD `/Volumes/ZX20`)

- **Window:** 2026-11-21, 13:03–14:05Z. Base and candidate alternated in one session, under the Docker lane with BENCH.
- **Not comparable** with BENCH2 or NS11: those used the old storage.
- **Load:** every step waited up to 10 min for a 1-minute load below 3, then ran. The load at each step's start and
  end is in `load.txt`.
- **Failed first attempts:** the first get pair failed for environmental reasons and was rerun. One run hit a Docker
  API 500 while creating the network; the other hit a verification read timeout at load 52.

### Serial update (0.2/s, 10 updates per run), means in ms

| stage | base | cand | cand2 | base2 |
|---|---:|---:|---:|---:|
| client p50 (median of phases) | 27.08 | 27.82 | 26.49 | 20.23 |
| client p99 (median of phases) | 27.08 | 27.82 | 26.49 | 20.23 |
| api http.dispatch.total | 24.846 | 29.014 | 23.117 | 18.472 |
| api gw.direct.send | 24.651 | 28.583 | 22.773 | 18.288 |
| w1 rpc.direct.serve | 20.607 | 22.877 | 19.290 | 14.588 |
| w1 leader.mutation.total | 20.586 | 22.863 | 19.276 | 14.575 |
| w1 leader.mutation.epochs | 0.772 | – | – | 0.548 |
| w1 leader.mutation.table | 0.011 | 0.014 | 0.010 | 0.010 |
| w1 leader.attempt.barrier | 0.180 | 0.021 | 0.041 | 0.177 |
| w1 raft.data.lease_index | 0.162 | 0.004 | 0.023 | 0.162 |
| w1 leader.attempt.read | 0.604 | 0.586 | 0.870 | 0.648 |
| w1 leader.attempt.timestamp_fetch | 4.533 | 5.526 | 3.991 | 3.982 |
| w1 leader.attempt.timestamp | 3.555 | 4.755 | 2.894 | 3.020 |
| w1 ts.client.call | 4.529 | 5.521 | 3.986 | 3.977 |
| w2 ts.serve.total | 0.588 | 0.098 | 0.063 | 0.678 |
| w2 ts.allocate.barrier | 0.533 | 0.015 | 0.015 | 0.612 |
| w1 leader.attempt.propose | 15.031 | 16.926 | 15.157 | 9.942 |
| w1 raft.data.client_write | 15.014 | 16.766 | 14.754 | 10.240 |
| w1 native.query.run | 5.00/op × 0.474 | 5.50/op × 0.649 | 5.50/op × 0.507 | 5.50/op × 0.459 |
| w1 native.control.run | 207.90/op × 0.529 | 204.10/op × 0.532 | 207.50/op × 0.483 | 206.90/op × 0.458 |
| verified | True | True | True | True |

| Derived (ms) | base | cand | cand2 | base2 |
| --- | ---: | ---: | ---: | ---: |
| Pre-commit path: `leader.mutation.total − client_write` | 5.57 | 6.10 | 4.52 | 4.34 |
| Timestamp transport: `ts.client.call − ts.serve.total` (not changed) | 3.94 | 5.42 | 3.92 | 3.30 |
| Pre-commit path without the timestamp transport | 1.63 | 0.68 | 0.60 | 1.04 |
| Worker CPU in the measured windows (w1 / w2 / w3) | 0.100 / 0.059 / 0.066 | 0.099 / 0.060 / 0.065 | 0.090 / 0.057 / 0.052 | 0.089 / 0.055 / 0.058 |

Reading:
- **Removed steps.** Every step RD1 removes is absent or near zero in both candidate runs:
  - `leader.mutation.epochs`: 0.55–0.77 ms, now 0 calls;
  - the Data lease barrier: 0.18 → 0.02–0.04 ms;
  - the Timestamp leader's barrier: 0.53–0.61 → 0.015 ms.
- **Client p50 does not show the saving.** The client p50 moves with stages RD1 does not touch:
  - the Data commit (`client_write`): 10.2–16.8 ms, mostly the follower fsync on the USB SSD;
  - the Timestamp RPC's transport: 3.3–5.4 ms.
- **Normalized path.** Without the timestamp transport, the candidate's pre-commit path is 0.60–0.68 ms against
  1.04–1.63 ms on the base.
- **CPU.** Worker CPU is equal between base and candidate.

### Point gets (sweep, 32 clients)

| Offered/s | base p50 / p99 | cand p50 / p99 |
| ---: | --- | --- |
| 1 | 5.02 / 6.69 | 4.77 / 7.30 |
| 2 | 4.18 / 7.11 | 5.72 / 8.53 |
| 5 | 4.99 / 12.21 | 6.43 / 9.46 |
| 20 | 5.46 / 10.19 | 5.32 / 9.38 |
| 50 | 4.70 / 11.18 | 4.63 / 22.19 |
| 100 | 4.44 / 12.84 | 4.80 / 25.31 |
| 200 | 4.72 / 83.63, 4 rejected (stops) | 4.35 / 40.25, 0 rejected |
| 500 | – | 104.8 / 273.2, 1,097 driver-capped, 23 rejected (stops) |

Stage means at 1–20/s:

| Stage (ms) | base | cand |
| --- | ---: | ---: |
| `leader.get.barrier` | 0.186 | 0.027 |
| `raft.data.lease_index` | 0.164 | 0.007 |
| `leader.get.read` (1 Query job) | 0.680 | 0.752 |
| `leader.get.total` | 0.932 | 0.859 |
| API `http.dispatch.total` | 2.478 | 2.471 |

Both runs verified, with 0 unresolved outcomes. The p50 differences at 2 and 5/s sit outside the server: API dispatch is
equal.

## GitHub runner (`[ci perf]`, serial and 50/s updates)

- **Base:** `28ca98c`, run 44895743781, attempts 1 and 2.
- **Candidate:** `f2e2199`, run 44897050330, attempts 1 and 2.
- Each attempt ran on a different runner.

### Serial (0.2/s, 10 updates)

| stage | base1 | base2 | cand1 | cand2 |
|---|---:|---:|---:|---:|
| client p50 (median of phases) | 4.37 | 3.74 | 5.04 | 4.77 |
| client p99 (median of phases) | 4.37 | 3.74 | 5.04 | 4.77 |
| api http.dispatch.total | 3.286 | 2.999 | 4.035 | 3.574 |
| api gw.direct.send | 3.209 | 2.918 | 3.936 | 3.493 |
| w1 rpc.direct.serve | 2.507 | 2.182 | 2.957 | 2.578 |
| w1 leader.mutation.total | 2.499 | 2.175 | 2.942 | 2.570 |
| w1 leader.mutation.epochs | 0.069 | 0.060 | – | – |
| w1 leader.mutation.table | 0.005 | 0.004 | 0.006 | 0.005 |
| w1 leader.attempt.barrier | 0.039 | 0.051 | 0.012 | 0.010 |
| w1 raft.data.lease_index | 0.032 | 0.044 | 0.003 | 0.002 |
| w1 leader.attempt.read | 0.125 | 0.133 | 0.156 | 0.152 |
| w1 leader.attempt.timestamp_fetch | 0.762 | 0.702 | 0.820 | 0.807 |
| w1 leader.attempt.timestamp | 0.524 | 0.453 | 0.564 | 0.569 |
| w1 ts.client.call | 0.759 | 0.700 | 0.815 | 0.805 |
| w2 ts.serve.total | 0.125 | 0.124 | 0.036 | 0.036 |
| w2 ts.allocate.barrier | 0.098 | 0.103 | 0.008 | 0.008 |
| w1 leader.attempt.propose | 1.608 | 1.367 | 2.062 | 1.709 |
| w1 raft.data.client_write | 1.601 | 1.362 | 2.054 | 1.675 |
| w1 native.query.run | 5.00/op × 0.046 | 5.00/op × 0.042 | 5.00/op × 0.056 | 5.50/op × 0.049 |
| w1 native.control.run | 208.90/op × 0.035 | 208.10/op × 0.035 | 207.30/op × 0.044 | 209.60/op × 0.043 |
| verified | True | True | True | True |

### 50/s (3000 updates)

| stage | base1 | base2 | cand1 | cand2 |
|---|---:|---:|---:|---:|
| client p50 (median of phases) | 2.48 | 2.20 | 3.20 | 3.06 |
| client p99 (median of phases) | 4.33 | 28.54 | 5.54 | 5.55 |
| api http.dispatch.total | 2.261 | 2.458 | 2.643 | 2.400 |
| api gw.direct.send | 2.130 | 2.181 | 2.553 | 2.315 |
| w1 rpc.direct.serve | 1.905 | 1.973 | 2.242 | 2.008 |
| w1 leader.mutation.total | 1.899 | 1.968 | 2.233 | 1.999 |
| w1 leader.mutation.epochs | 0.066 | 0.060 | – | – |
| w1 leader.mutation.table | 0.003 | 0.003 | 0.005 | 0.005 |
| w1 leader.attempt.barrier | 0.042 | 0.118 | 0.024 | 0.021 |
| w1 raft.data.lease_index | 0.037 | 0.113 | 0.017 | 0.013 |
| w1 leader.attempt.read | 0.111 | 0.108 | 0.156 | 0.168 |
| w1 leader.attempt.timestamp_fetch | 0.312 | 0.283 | 0.321 | 0.330 |
| w1 leader.attempt.timestamp | 0.119 | 0.101 | 0.088 | 0.088 |
| w1 ts.client.call | 0.307 | 0.278 | 0.314 | 0.323 |
| w2 ts.serve.total | 0.109 | 0.093 | 0.032 | 0.032 |
| w2 ts.allocate.barrier | 0.091 | 0.078 | 0.008 | 0.008 |
| w1 leader.attempt.propose | 1.478 | 1.504 | 1.844 | 1.602 |
| w1 raft.data.client_write | 1.473 | 1.500 | 1.836 | 1.595 |
| w1 native.query.run | 5.00/op × 0.046 | 4.99/op × 0.036 | 5.00/op × 0.052 | 5.00/op × 0.048 |
| w1 native.control.run | 2.75/op × 0.054 | 2.73/op × 0.051 | 2.73/op × 0.073 | 2.72/op × 0.065 |
| verified | True | True | True | True |

Container CPU in the measured windows (`cpu.py`), with base 1, base 2, cand 1 and cand 2 in order:

```
base/rate50 3000 w1 0.106cpu w2 0.070cpu w3 0.061cpu api 0.019cpu
base2/rate50 3000 w1 0.100cpu w2 0.066cpu w3 0.057cpu api 0.020cpu
cand/rate50 3000 w1 0.136cpu w2 0.087cpu w3 0.080cpu api 0.027cpu
cand2/rate50 3000 w1 0.127cpu w2 0.080cpu w3 0.072cpu api 0.026cpu
base/serial 10 w1 0.018cpu w2 0.012cpu w3 0.012cpu api 0.006cpu
base2/serial 10 w1 0.021cpu w2 0.015cpu w3 0.015cpu api 0.008cpu
cand/serial 10 w1 0.024cpu w2 0.016cpu w3 0.016cpu api 0.008cpu
cand2/serial 10 w1 0.025cpu w2 0.018cpu w3 0.018cpu api 0.008cpu
```

Both candidate runners were slower on code RD1 does not touch:
- native job time;
- `client_write`;
- the API process, which runs no Raft groups, used 30% more CPU.

The same binaries show equal worker CPU on the controlled host. The perf logs record no CPU model.

An in-process A/B isolated `enable_local_lease_read`. It used one debug build, a real 3-voter Data group over mTLS, and
400 update+get pairs, with the flag on and off interleaved three times each:

| | flag on | flag off |
| --- | --- | --- |
| CPU per pair (ms) | 50.4, 51.4, 48.8 | 50.2, 47.7, 47.5 |
| Idle process CPU | 0.130, 0.134, 0.127 | 0.147, 0.126, 0.104 |
| Get p50 (ms) | 1.41, 1.42, 1.39 | 1.48, 1.43, 1.44 |

## Files

- `compare.py`: the stage tables.
- `cpu.py`: container CPU in the measured windows.
- `runs/*.json`: compact records of the six host runs (statuses, latencies and the stage counts and sums used above).
- `load.txt`: the host load at each step's start and end.
