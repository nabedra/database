# Native storage and direct OLTP path: performance gate (NS11, NS18, T10)

This is the section 8 gate of [the native storage plan](../../../../../plans/native-storage-engine.md) and the
"Benchmark" section of [the direct-path plan](../../../../../plans/oltp-direct-path.md). It ran on 2026-11-20 on the
controlled host, one system at a time. The verdict is at the top; the tables and method follow.

## Verdict

The final candidate is lead `20747f8` (BLD5 and FINALfix), release, `--no-default-features --features oltp`, with
`direct_oltp` on. The baseline is `b29a178` (RocksDB). The gate does not pass. G7 misses, G1 misses on the queued
path for every Nabedra build including the baseline, and G6 is only partly measurable. All three direct-path
targets miss. G2 to G5 pass by wide margins.

| # | Criterion | Candidate `20747f8` direct on | Baseline `b29a178` | Result |
| --- | --- | --- | --- | --- |
| G1 | Zero snapshot mismatches and unresolved outcomes | 0 mismatches and 0 unresolved in serial, update, get, mix and soak (500 sampled keys) | 0 in the stop-rule sweeps; 2 mismatches in the continuation sweep and 1 in the fsync run | Miss. A queued-path 429 `worker outgoing lane full` with `outcome: not_applied` was applied (6 cases, see "False not_applied") |
| G2 | Median acknowledged throughput at least the baseline's | 0.20, 1.00 and 2.00 updates/s at 0.2, 1 and 2/s | 0.20, 0.77 and 1.00 | Pass at every rate the baseline ran |
| G3 | Median successful p99 at most 1.05 x the baseline's | 47.8, 51.6 and 55.2 ms at 0.2, 1 and 2/s | 1,153, 3,983 and 4,799 ms | Pass |
| G4 | fsync + fdatasync per acknowledged write | 7.9 at 1/s, 3.7 over 1, 5 and 20/s | 162 at 1/s | Pass |
| G5 | cgroup `io.stat` `wbytes` per acknowledged write | 40 KB at 1/s (strace run), 14.0 KB in the update sweep | 882 KB at 1/s (strace run), 814 KB in the update sweep | Pass |
| G6 | Recovery at most max(1 s, baseline) | In-place restart of one SIGKILLed worker refused; cold restart of all three: workers ready in 7.3 s and 7.9 s, the running API never served again within 180 s | In-place restart refused; cold restart: workers ready in 8.5 s twice (no rows, then 20 MB) | Partial. Worker readiness passes (the baseline had less data); serving fails |
| G7 | Largest worker `memory.peak` at most 1.05 x the baseline's, zero OOM | 176.3 MB, 0 OOM | 108.6 MB, 0 OOM | Miss (1.62 x). Anon memory alone is 1.26 x on the leader |

| Direct-path target | Candidate `20747f8` direct on | ScyllaDB 6.2.3 | Result |
| --- | --- | --- | --- |
| Serial update p50 at most 15 ms | 47.8 ms | 7.6 ms | Miss |
| Zero rejections through 200 updates/s with p99 at most 25 ms | p99 51.6 ms at 1/s; the sweep stops after 50/s (p99 109 ms) | Clean through 200/s, p99 33.9 ms | Miss |
| Within 20% of ScyllaDB | 6.3 x ScyllaDB's serial p50 | | Miss |

Nabedra's `table.update` reads the row before it writes and returns whether the row existed. ScyllaDB only does that
with a lightweight transaction (Paxos); the ScyllaDB numbers use a plain prepared `UPDATE` at QUORUM.

## Systems and caps

| System | Binary | Notes |
| --- | --- | --- |
| Baseline | `nabedra-db-b29a178`, SHA-256 `953a35f3…e1012e` | `--pool-format-version 1` and the `b29a178` Python ORM tree for the API CLI |
| Candidate (final) | `nabedra-db-c74c2d4`, SHA-256 `bf370a7e…2015ac2`; server sources identical to lead `20747f8` | Built on Actions in `rust:1.94.1-bookworm` (glibc 2.36), [run 44811636932](https://github.com/nabedra/database/actions/runs/44811636932), 426 s |
| Candidate (earlier) | `nabedra-db-83d7a0b`, SHA-256 `261f8521…efb1d5`; sources identical to `a7f03c2` | [Run 44804204143](https://github.com/nabedra/database/actions/runs/44804204143), 443 s; the e7453a1 build produced the same SHA-256 |
| ScyllaDB | `scylladb/scylla:6.2.3@sha256:a9d90408…` | RF3, QUORUM, `--commitlog-sync=batch`, `--smp=1`, `--memory=1700M` |

All Nabedra runs used the image `nabedra-oltp-perf-builder:1.94.1` (`sha256:1547483c…`), the same image as the
earlier latency evidence. It sets `TOKIO_WORKER_THREADS=2`, which applies to every Nabedra system equally. The fsync
runs used `nabedra-ns11-strace:1.94.1`, which is that image plus Debian's strace 6.1. Docker Desktop 28.3.3 ran
Linux 6.10.14-linuxkit with 8 vCPUs, 8.22 GB and cgroup v2.

Every Nabedra run checked the caps through Docker inspection and each container's `cpu.max`. Workers had 1 CPU
(`100000 100000`), 2.15 GB and swap equal to memory. The API had 0.35 CPU (`35000 100000`) and 550 MB, and the
driver had 0.65 CPU and 900 MB, 4 CPUs and 7.9 GB in total. No run recorded an OOM kill or a cgroup `oom` event.
The ScyllaDB runs used three 1 CPU, 2.15 GB nodes and the same driver cap, with no API container.

The host load average was 2 to 10 during the runs. Another agent ran an Events debug test binary from worktree
`04dba262…` at about 0.8 to 0.9 CPU during part of the matrix. `ns11/load.log` in the ledger records the load next to
every run.

## Workloads

All runs used `--seed 20261118` and, unless stated, `--timeout 10`.

| Workload | Arguments |
| --- | --- |
| Serial pair | `--rates 0.2 --seconds 5 --warmup-seconds 5 --repetitions 10 --clients 1 --rows 2000` |
| Update sweep | `--rates 1,2,5,20,50,100,200,500,1000 --seconds 5 --warmup-seconds 3 --repetitions 3 --clients 32 --rows 5000 --stop-p99-ms 100 --stop-on-rejection` |
| Point-get sweep | the update sweep with `--operation get` |
| 95/5 mix | the update sweep with `--operation mix --read-fraction 0.95` |
| Update continuation (extra) | `--rates 1,2,5,20,50 --seconds 5 --warmup-seconds 3 --repetitions 3 --clients 32 --rows 5000`, no stop rule, so G2 and G3 have baseline points above 1/s |
| fsync run | the update sweep's arguments with `--rates 1,5,20 --rows 2000 --timeout 60`, no stop rule, every worker under `strace -f --seccomp-bpf -ttt -T -e trace=fsync,fdatasync,sync_file_range,syncfs` |
| Storage soak | `--soak --soak-rows 80000 --soak-payload-bytes 2048 --soak-seconds 600 --soak-phase-seconds 60 --soak-rate 50 --clients 32 --soak-verify-keys 500 --cold-restart` |

The sweeps stop after the first rate whose median p99 exceeds 100 ms or that has any rejection. The get and mix
sweeps first ran with 10,000 rows and rates up to 2,000/s. Both candidate runs failed before the first phase: the
snapshot check reads every row through the queued `query.collect` and `query.count`, and 10,000 rows exceeded the 10 s
client timeout. The reruns use 5,000 rows, the same key space as the update sweep. The checks after each phase take
9 to 12 s on the candidate, 15 to 18 s on the baseline and 0.1 s on ScyllaDB.

Gets must return the previous snapshot's value or a value whose write is still unresolved, because the keys within one
phase are distinct. Soak rows carry an incompressible 2 KiB string, so 80,000 rows are 165 MB of logical data, more
than 4 x the worker's 16 MiB cache plus 16 MiB write buffer. Each candidate worker held 1.10 GB on disk after the soak.

## Results

Each cell gives acknowledged/offered writes or reads over the measured repetitions, the median acknowledged rate,
and the medians of the per-repetition p50 and p99 in ms, measured from scheduled arrival. `rej` counts explicit
429 rejections, `cap` arrivals refused by the driver's 32-client limit, and `unres` unresolved outcomes.

#### Serial pair

| System | Stopped after | Verified |
| --- | ---: | --- |
| b29a178 | – | True |
| 20747f8 direct on | – | True |
| a7f03c2 direct on | – | True |
| 20747f8 direct off | – | True |
| a7f03c2 direct off | – | True |
| ScyllaDB 6.2.3 | – | True |

| Offered/s | b29a178 | 20747f8 direct on | a7f03c2 direct on | 20747f8 direct off | a7f03c2 direct off | ScyllaDB 6.2.3 |
| ---: | --- | --- | --- | --- | --- | --- |
| 0.2 | 10/10 · 0.20/s · p50 1,153.3 · p99 1,153.3 | 10/10 · 0.20/s · p50 47.8 · p99 47.8 | 10/10 · 0.20/s · p50 46.5 · p99 46.5 | 10/10 · 0.20/s · p50 511.6 · p99 511.6 | 10/10 · 0.20/s · p50 530.4 · p99 530.4 | 10/10 · 0.20/s · p50 7.6 · p99 7.6 |

#### Update sweep

| System | Stopped after | Verified |
| --- | ---: | --- |
| b29a178 | 1 | True |
| 20747f8 direct on | 50 | True |
| a7f03c2 direct on | 50 | True |
| 20747f8 direct off | 1 | True |
| a7f03c2 direct off | 1 | True |
| ScyllaDB 6.2.3 | 500 | True |

| Offered/s | b29a178 | 20747f8 direct on | a7f03c2 direct on | 20747f8 direct off | a7f03c2 direct off | ScyllaDB 6.2.3 |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | 15/15 · 0.77/s · p50 3,272.0 · p99 3,983.4 | 15/15 · 1.00/s · p50 38.0 · p99 51.6 | 15/15 · 1.00/s · p50 37.9 · p99 50.6 | 15/15 · 1.00/s · p50 564.3 · p99 970.6 | 15/15 · 1.00/s · p50 521.4 · p99 949.4 | 15/15 · 1.00/s · p50 7.4 · p99 10.7 |
| 2 | – | 30/30 · 2.00/s · p50 36.3 · p99 55.2 | 30/30 · 2.00/s · p50 35.6 · p99 48.9 | – | – | 30/30 · 2.00/s · p50 4.9 · p99 10.4 |
| 5 | – | 75/75 · 5.00/s · p50 34.3 · p99 69.9 | 75/75 · 5.00/s · p50 31.8 · p99 49.7 | – | – | 75/75 · 5.00/s · p50 6.1 · p99 11.0 |
| 20 | – | 300/300 · 20.00/s · p50 32.3 · p99 64.4 | 300/300 · 20.00/s · p50 35.7 · p99 98.3 | – | – | 300/300 · 20.00/s · p50 5.3 · p99 15.1 |
| 50 | – | 750/750 · 49.53/s · p50 35.6 · p99 109.5 | 749/750 · 49.32/s · p50 57.3 · p99 186.6, rej 1 | – | – | 750/750 · 50.00/s · p50 5.1 · p99 50.6 |
| 100 | – | – | – | – | – | 1500/1500 · 99.96/s · p50 5.2 · p99 39.8 |
| 200 | – | – | – | – | – | 3000/3000 · 200.00/s · p50 5.2 · p99 33.9 |
| 500 | – | – | – | – | – | 7047/7500 · 480.10/s · p50 24.9 · p99 131.6, cap 453 |

#### Point-get sweep

| System | Stopped after | Verified |
| --- | ---: | --- |
| b29a178 | 1 | True |
| 20747f8 direct on | 100 | True |
| a7f03c2 direct on | 100 | True |
| 20747f8 direct off | 1 | True |
| a7f03c2 direct off | 1 | True |
| ScyllaDB 6.2.3 | 500 | True |

| Offered/s | b29a178 | 20747f8 direct on | a7f03c2 direct on | 20747f8 direct off | a7f03c2 direct off | ScyllaDB 6.2.3 |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | 15/15 · 1.00/s · p50 605.3 · p99 887.9 | 15/15 · 1.00/s · p50 17.2 · p99 22.0 | 15/15 · 1.00/s · p50 17.2 · p99 20.3 | 15/15 · 1.00/s · p50 183.2 · p99 222.7 | 15/15 · 1.00/s · p50 214.5 · p99 229.7 | 15/15 · 1.00/s · p50 6.6 · p99 9.6 |
| 2 | – | 30/30 · 2.00/s · p50 14.5 · p99 19.4 | 30/30 · 2.00/s · p50 14.5 · p99 19.4 | – | – | 30/30 · 2.00/s · p50 7.0 · p99 9.1 |
| 5 | – | 75/75 · 5.00/s · p50 16.3 · p99 21.2 | 75/75 · 5.00/s · p50 11.7 · p99 26.7 | – | – | 75/75 · 5.00/s · p50 8.2 · p99 13.2 |
| 20 | – | 300/300 · 20.00/s · p50 14.1 · p99 24.6 | 300/300 · 20.00/s · p50 10.5 · p99 19.8 | – | – | 300/300 · 20.00/s · p50 7.3 · p99 10.2 |
| 50 | – | 750/750 · 49.97/s · p50 11.8 · p99 26.4 | 750/750 · 49.99/s · p50 11.2 · p99 25.8 | – | – | 750/750 · 50.00/s · p50 7.2 · p99 11.7 |
| 100 | – | 1192/1500 · 77.73/s · p50 93.0 · p99 601.6, rej 191, cap 117 | 1399/1500 · 86.07/s · p50 228.1 · p99 556.7, cap 101 | – | – | 1500/1500 · 99.94/s · p50 6.7 · p99 12.7 |
| 200 | – | – | – | – | – | 3000/3000 · 200.00/s · p50 5.5 · p99 19.6 |
| 500 | – | – | – | – | – | 7259/7500 · 477.75/s · p50 28.5 · p99 102.5, cap 241 |

#### 95/5 mix (all operations; reads and writes separately in runs/mix-*.json)

| System | Stopped after | Verified |
| --- | ---: | --- |
| b29a178 | 1 | True |
| 20747f8 direct on | 100 | True |
| a7f03c2 direct on | 100 | False |
| 20747f8 direct off | 1 | True |
| a7f03c2 direct off | 1 | True |
| ScyllaDB 6.2.3 | 500 | True |

| Offered/s | b29a178 | 20747f8 direct on | a7f03c2 direct on | 20747f8 direct off | a7f03c2 direct off | ScyllaDB 6.2.3 |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | 15/15 · 1.00/s · p50 594.1 · p99 932.4 | 15/15 · 1.00/s · p50 17.6 · p99 21.4 | 15/15 · 1.00/s · p50 17.8 · p99 21.4 | 15/15 · 1.00/s · p50 183.1 · p99 241.0 | 15/15 · 1.00/s · p50 201.1 · p99 292.5 | 15/15 · 1.00/s · p50 8.3 · p99 8.7 |
| 2 | – | 30/30 · 2.00/s · p50 14.0 · p99 18.6 | 30/30 · 2.00/s · p50 16.0 · p99 19.4 | – | – | 30/30 · 2.00/s · p50 7.0 · p99 9.3 |
| 5 | – | 75/75 · 5.00/s · p50 17.2 · p99 47.1 | 75/75 · 5.00/s · p50 17.5 · p99 49.0 | – | – | 75/75 · 5.00/s · p50 8.2 · p99 10.2 |
| 20 | – | 300/300 · 20.00/s · p50 13.7 · p99 48.5 | 300/300 · 20.00/s · p50 13.4 · p99 45.3 | – | – | 300/300 · 20.00/s · p50 7.6 · p99 16.4 |
| 50 | – | 750/750 · 49.98/s · p50 11.7 · p99 38.6 | 750/750 · 50.00/s · p50 12.0 · p99 60.5 | – | – | 750/750 · 49.99/s · p50 7.4 · p99 12.2 |
| 100 | – | 1020/1500 · 66.21/s · p50 219.3 · p99 601.9, rej 389, cap 91 | 1381/1500 · 92.53/s · p50 40.6 · p99 535.0, cap 119 | – | – | 1500/1500 · 99.98/s · p50 6.9 · p99 40.4 |
| 200 | – | – | – | – | – | 3000/3000 · 200.00/s · p50 5.4 · p99 16.4 |
| 500 | – | – | – | – | – | 7101/7500 · 471.38/s · p50 25.8 · p99 114.8, cap 399 |

#### Update continuation, no stop rule (extra)

| System | Stopped after | Verified |
| --- | ---: | --- |
| b29a178 | – | False (5-warmup snapshot verification failed: {'rows') |
| 20747f8 direct off | – | False (20-rep1 snapshot verification failed: {'rows':) |
| a7f03c2 direct off | – | False (50-rep1 snapshot verification failed: {'rows':) |

| Offered/s | b29a178 | 20747f8 direct off | a7f03c2 direct off |
| ---: | --- | --- | --- |
| 1 | 14/15 · 0.90/s · p50 2,963.1 · p99 3,445.0, unres 1 | 15/15 · 1.00/s · p50 538.5 · p99 565.7 | 15/15 · 1.00/s · p50 516.4 · p99 548.0 |
| 2 | 26/30 · 1.01/s · p50 4,052.9 · p99 4,799.3, unres 4 | 30/30 · 2.00/s · p50 514.5 · p99 996.9 | 30/30 · 2.00/s · p50 508.6 · p99 602.2 |
| 5 | – | 74/75 · 3.65/s · p50 1,969.4 · p99 2,534.0, rej 1 | 75/75 · 3.66/s · p50 2,004.1 · p99 2,553.5 |
| 20 | – | 39/100 · 3.99/s · p50 4,382.4 · p99 5,905.2, rej 61 | 120/300 · 3.99/s · p50 4,751.6 · p99 5,815.4, rej 180 |
| 50 | – | – | 39/250 · 3.91/s · p50 5,091.0 · p99 5,598.5, rej 211 |

### Where the time goes

At the serial rate, the leader reports 27 to 46 ms from admission to reply for a direct update, of the client's
about 45 ms. Under strace, `fdatasync` takes 1.1 to 1.2 ms at p50 and 2.6 to 3.3 ms at p99 on this VM, and the
candidate issues about 1.24 per acknowledged write per worker. Sync time therefore explains only a few ms of each
commit. ScyllaDB's 7.6 ms on the same disk agrees.

CPU is the limit for updates. Between 1 and 50/s, the leader's CPU use grows by about 6.7 ms per update and the three
workers' by about 14 ms. At 50/s the leader runs at 0.87 of its 1 CPU and is throttled in 24 of 51 periods. A point
get costs under 1 ms of worker CPU, and the leader's admission-to-reply time is 2.1 ms on average. Above about 80
requests/s the Python HTTPS driver becomes the limit: at 100 gets/s it used 0.59 of its 0.65 CPU and was throttled in
37 of 53 periods, while ScyllaDB's CQL driver reached 200/s at 0.32 CPU. The candidate rows at 100/s therefore
measure the driver, not the server.

### Direct-path counters

No run recorded a direct fallback of any reason. Every proposed entry carried one row (`batch_sizes: {le_1}`). In the
sweeps the only nonzero failure counters were one latch wait per update sweep. The soak, the second candidate run,
recorded `retries: {leadership: 9}`, `proposal_failures: {stalled: 2, leadership: 2}` and `not_leader: 8`, so the
Data leadership moved during flush and compaction. With `direct_oltp` off, the leaders record only forward markers.
The API's `direct_gateway` counters stayed at zero in every run; its `/v1/runtime/stats` does not seem to read the
gateway that served the requests.

`worker outgoing lane full` refusals reach clients only on the queued path (direct off and the baseline). They
appear in the continuation and fsync runs at 5 updates/s and above. With direct on, every refusal was a 429
`executor admission capacity exhausted` from the API's HTTP executor (`server/src/executor.rs:87`). At 100 req/s,
`20747f8` returned 223 of them in the get sweep and 448 in the mix, against 0 and 17 for `a7f03c2`. At 50/s and below
the two builds match. The plausible link is FINALfix L1: outgoing calls now wait for a per-class semaphore of 4 x the
handler slots, so admitted requests stay in the gateway longer. The driver limit at that rate keeps this unproven.

### False not_applied

The continuation and fsync runs found rows holding a value the client was told had not been written. In all six
cases the update had received HTTP 429 `{"code": "overloaded", "message": "worker outgoing lane full", "outcome":
"not_applied"}`, and the value is in the next snapshot.

| Run | Lane-full refusals | Applied anyway |
| --- | ---: | ---: |
| `b29a178`, continuation | 3 | 2 |
| `b29a178`, fsync run | 1 | 1 |
| `a7f03c2` direct off, continuation | 6 | 1 |
| `20747f8` direct off, continuation | 2 | 1 |
| `20747f8` direct off, fsync run | 1 | 1 |

A run stops at its first mismatch, so the counts are lower bounds. `From<RequestError> for ProtocolError` in
`server/src/oltp_replication/multiplex/transport.rs` maps every `OutgoingLaneFull` to `overloaded` with `not_applied`.
A queued request that has already committed and then meets a full lane on a later RPC therefore reports a write that
happened as one that did not. The direct path did not show it at the measured rates.

### Soak

| | `20747f8` direct on (2 runs) | `b29a178` |
| --- | --- | --- |
| Seeding, 80,000 x 2 KiB rows, 4 clients | 57 s, no rejections | Failed. The API closed the TLS connection on 256 KiB `insert_many` requests (`SSLEOFError`), whose outcomes are then unknown |
| 10 x 60 s at 50 updates/s | 28,536 and 28,792 of 30,000 acknowledged; 1,347 and 1,107 rejected (`executor admission capacity exhausted`); phase p99 161 to 2,928 ms | Not run. With one seeding client the baseline wrote about 0.3 MB a minute, one 256 KiB batch, so seeding 165 MB would take hours; stopped after 8 minutes |
| Verification | Run 2: 0 mismatches in 500 sampled keys. Run 1's check was a driver bug (it expected no `payload` column) and is not counted | |
| Write amplification on each worker | flush 75 MB + compaction 890 MB + journal 234 MB for 166 MB of user bytes, about 7.2 x; `stall_ms` 2 to 74 | |
| Bytes written per acknowledged write | 259 KB (three workers) | |
| Memory | anon 104 to 111 MB, page cache up to 1.3 GB per worker | |

So the candidate does not sustain 50 updates/s once flush and compaction run. 50/s was the highest rate without
rejections in the update sweep.

### Recovery

A registered worker that was SIGKILLed cannot rejoin its run in place. Both `b29a178` and the candidate refuse the
restart with `registered startup ValidationError: invalid hosted group placement or identity`. The design restarts
a worker through `worker.restart.prepare` before a planned stop, or replaces a failed one. G6 is therefore measured
as a cold restart of all three workers onto a new run (R04, D09). Both builds have that path.

| | `20747f8` after soak 1 | `20747f8` after soak 2 | `b29a178` (two runs, soak seeding stopped) |
| --- | --- | --- | --- |
| All workers ready on the new run | 7.3 s | 7.9 s | 8.5 s unseeded, 8.5 s with 20 MB |
| `data.direct.v1` active again | 7.5 s | 8.1 s | |
| First point get through the running API | none in 180 s (503 `storage_unavailable`, "direct request deadline exceeded before execution") | same | not probed (no table) |

GATEfix follow-up (2026-11-21). The running API did not recover because every worker came back as a new
incarnation and, in this driver, with a new certificate. A receiver refused any request addressed to another
incarnation, so the API could not read the new enrollments from Control. A receiver now accepts a Control
authority call from an enrolled API that names an older incarnation of the same worker and certificate. The
`[ci direct]` harness scenario G6 (in the D09 cluster) kills every worker, keeps both gateways running and
cold-starts the workers:

- Same certificates (a restarted container): both running gateways served a point get 4.3 s after the
  cold start began, as the workers became ready. With the receiver check of `1ea4622`, the same scenario
  failed: no point get within 60 s (503 `outcome_unknown`).
- New certificates (this driver): no peer pinned in a running gateway remains, so it needs the new cluster
  connection. It answers 503 `storage_unavailable` "direct request deadline exceeded before execution"
  (`outcome: not_applied`) until it is restarted with the new `control_peers`; then it serves at once.

A plain restart of one SIGKILLed registered worker with its previous configuration is refused by design:
`registered startup ValidationError: invalid hosted group placement or identity` comes from the retained
identity check (`NABEDRA_REGISTERED_WORKER.json`), which requires a higher enrollment revision and a new
incarnation. The restart path after a crash is `worker.restart.prepare`, sent to a live worker once Control has
removed the crashed worker from the ready set, followed by a start with the prepared (rolling) enrollment; or
a replacement. NS11 and GATEfix did not run that path.

### Memory (G7)

The largest worker `memory.peak` over the serial, update, get, mix and fsync runs is 108.6 MB for the baseline and
176.3 MB for the candidate. The samples that include `memory.stat` split it:

| | Leader anon | Follower anon | Page cache |
| --- | ---: | ---: | ---: |
| `b29a178` | 55.7 to 59.8 MiB | 50.0 to 56.3 MiB | 12.6 to 41.7 MiB |
| `20747f8` direct on | 53.3 to 75.5 MiB | 45.2 to 48.8 MiB | 72.6 to 90.6 MiB |

Each candidate worker writes 69.6 MB at startup against 1.9 MB for the baseline. That is the zero-filled 64 MiB
journal segment, and its pages stay charged to the cgroup. Releasing them after the zero-fill (for example
`posix_fadvise(DONTNEED)` once the segment is synced) would close most of the gap; the leader's extra anon memory
would remain.

## NS18 microbench

`ns18_bench_native_microbench` in `server/kv/tests/store_bench.rs` runs on Actions (`ubuntu-24.04`, 4 vCPUs) in the
`bench` job: `cargo test --release -p nabedra-kv --test store_bench -- --ignored --nocapture`. The numbers below are
from the `20747f8` build. GitHub's runner disk completes `fdatasync` in about 60 µs, much faster than a real device
or the Docker VM, so the synced numbers show engine overhead, not device latency. [ns18-20747f8.log](ns18-20747f8.log)
and [ns18-a7f03c2.log](ns18-a7f03c2.log) are the full outputs.

| Operation | Writers | Throughput | p50 | p99 | Journal syncs per commit |
| --- | ---: | ---: | ---: | ---: | ---: |
| Synced put, 100 B | 1 / 4 / 16 / 64 | 12.5k / 44.5k / 154k / 272k ops/s | 66 / 87 / 80 / 195 µs | 83 / 102 / 144 / 326 µs | 1.000 / 0.498 / 0.098 / 0.021 |
| Synced put, 1 KiB | 1 / 4 / 16 / 64 | 20.6k / 39.9k / 137k / 203k ops/s | 42 / 87 / 101 / 267 µs | 68 / 231 / 153 / 434 µs | 1.000 / 0.499 / 0.107 / 0.021 |
| Unsynced put, 100 B | 1 / 4 / 16 / 64 | 129k / 391k / 409k / 370k ops/s | 6 / 8 / 36 / 162 µs | 25 / 20 / 62 / 229 µs | 0 |
| Unsynced put, 1 KiB | 1 / 4 / 16 / 64 | 106k / 132k / 338k / 279k ops/s | 6 / 12 / 43 / 165 µs | 31 / 30 / 77 / 971 µs | 0 |
| Raft append, 64 x 1 KiB synced | 1 / 8 | 8.5k / 14.7k batches/s | 93 / 518 µs | 177 / 760 µs | 1.005 / 0.253 |
| Raft purge (`delete_range`) | 1 / 8 | 72k / 162k ops/s | 8 / 41 µs | 46 / 78 µs | |
| Point get, hot (second pass) / cold (1 MiB cache) | 1 | 767k / 254k ops/s | 1 / 3 µs | 2 / 6 µs | |
| Scan of 10, hot / cold | 1 | 356k / 169k ops/s | 2 / 5 µs | 3 / 9 µs | |
| Scan of 128, hot / cold | 1 | 101k / 54k ops/s | 9 / 17 µs | 19 / 34 µs | |

A 256 MiB install in 256 KiB unsynced batches took 1.65 s (155 MiB/s) and stalled writers for 733 ms. Over the whole
bench, the journal wrote 1.06 x the user bytes and journal, flush and compaction together 2.08 x; `stall_ms` totalled
802. Reopening after 20,000 unflushed puts took 22 ms, and peak RSS was 126 MB. One unsynced 1 KiB put with 4
writers took 69.6 ms, and the stall counter rose by 69 ms in that step. The same bench on the macOS host (APFS,
`F_FULLFSYNC`) gave a synced single-writer p50 of 20 ms, which is that filesystem's sync cost.

## Builds for the record

| Build | Time | Size | Source |
| --- | --- | --- | --- |
| Release `nabedra-db` (oltp), cold, 4 jobs, no cache | 401 to 443 s | 93.3 MB (`20747f8`), 95.8 MB (`a7f03c2`) | `bench` job, this branch |
| OLTP image, cold plus cache export | 515 s | 240 MB | [run 44733897949](https://github.com/nabedra/database/actions/runs/44733897949) (`bbc0ae0`) |
| OLTP image after a one-line change | 392 to 428 s | | ledger, BLD1 to BLD3 |
| Events image, cold plus cache export | 303 s | 109 MB | run 44733897949 |

The candidate's runtime libraries are `libc`, `libm`, `libgcc_s` and the loader.

## Files

`runs/*.json` holds one compact record per run, named `<workload>-<system>`. `base` is `b29a178`, `cand-on` and `cand-off` are `a7f03c2` with `direct_oltp` on and off, `cand2-on` and `cand2-off` are `20747f8`, and `scylla` is ScyllaDB. `soak-*-1` are the first attempts, and `soak-base-cold-restart.json` is the baseline's cold restart after its stopped soak. Each record has the settings, binary SHA-256, image, Docker environment, the per-rate
summary, direct counters, client-observed refusals, cgroup I/O and CPU over the measured window, strace counts,
worker memory and OOM events, cap inspection, and the soak and recovery records. The full outputs (per-request
samples, worker logs, cgroup samples, strace logs, about 50 MB) stay in the ledger under
`.nabedra/workflows/remove-rocksdb/ns11/runs/`.

The driver changes are in `scripts/benchmark_oltp_nabedra.py`, `scripts/benchmark_oltp_workload.py`,
`scripts/benchmark_scylla_oltp.py` and `scripts/benchmark_scylla_oltp.sh`. Their regression tests are in
`scripts/test_benchmark_oltp_workload.py`.
