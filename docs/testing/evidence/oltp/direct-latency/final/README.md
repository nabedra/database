# Final comparative benchmark of the follow-up PR (BENCH2, NS11 rerun)

Measured on 2026-11-21 between 07:14Z and 09:25Z on the NS11 controlled host, one system at a time. The candidate is the follow-up PR's
head `5eb80fb` (`nabedra/rr-latency`). It is compared with main (PR #20), with the RocksDB baseline `b29a178` and with ScyllaDB
6.2.3. The verdict comes first; the tables and method follow.

## Verdict

G1 to G6 pass against `b29a178`. G7 misses under the NS11 rule: the largest worker `memory.peak` over all runs is 1.16 x the
baseline's. That peak comes from the update sweep's 100 and 200/s phases, which the baseline never reached. For the same
workload, the candidate's peak is 0.67 to 0.87 x the baseline's.

Of the direct-path targets, gets pass: p50 is within 20% of ScyllaDB at every rate, and at most 6.6 ms at 5 of 7 rates. Every
update target misses. The serial update p50 is 20.6 ms against 9.9 ms for ScyllaDB the same morning. That is half of main's
41.2 ms but above the plan's 15 ms. The update sweep has no rejections through 100/s, but its p99 is 27 to 67 ms, and at 200/s
the server acknowledges only 99.5/s.

| # | Criterion | Candidate `5eb80fb` direct on | Baseline `b29a178` | Result |
| --- | --- | --- | --- | --- |
| G1 | Zero snapshot mismatches and unresolved outcomes | 0 mismatches, 0 unresolved in serial (with and without the snapshot between phases), update, get, mix, fsync run and both soaks (500 sampled keys each). Every refusal was a 429 `executor admission capacity exhausted` before execution, and none of them was applied. | 0 / 0 in serial, update, get and mix. The fsync run's final snapshot timed out under strace (60 s), so it is unverified. | Pass |
| G2 | Median acknowledged throughput at least the baseline's | 0.20 (serial), 1.00 (update, get, mix at 1/s); fsync run 1.00 / 5.00 / 20.00 | 0.20, 0.88, 1.00, 1.00; fsync run 0.12 / 0.03 / 0.09 | Pass |
| G3 | Median successful p99 at most 1.05 x the baseline's | 20.6 (serial), 29.1 (update 1/s), 9.4 (get), 10.7 (mix) ms | 1,143.5, 3,512.3, 685.0, 1,009.8 ms | Pass |
| G4 | fsync + fdatasync per acknowledged write (strace run) | 3.00 / 3.04 / 2.98 at 1 / 5 / 20/s (about one fdatasync per write per worker) | 226.9 / 485.9 / 284.6 | Pass |
| G5 | cgroup `io.stat` `wbytes` per acknowledged write | 21.6 KB at 1/s (strace run), 14.9 KB in the update sweep, 30.2 KB serial | 1,250 KB, 760 KB, 782 KB | Pass |
| G6 | Recovery at most max(1 s, baseline) | Cold restart of all three workers, same certificates: ready in 6.0 s after the update sweep; the running API served a point get at 6.5 s. After the soaks (1.10 and 1.23 GB per worker): ready in 5.8 and 7.0 s, served at 6.1 and 7.3 s. | Ready in 6.4 s after its update sweep; its running API never served within 180 s (503 `storage_unavailable` "worker transport unavailable") | Pass (readiness 6.0 vs 6.4 s on the same workload; serving recovers only on the candidate) |
| G7 | Largest worker `memory.peak` at most 1.05 x the baseline's, zero OOM | 126.0 MB (w1, update sweep, before the cold restart); 0 OOM | 108.6 MB (w1, fsync run); 0 OOM | Miss (1.16 x). Same workload: serial 0.76 x, fsync run 0.67 x, get 0.81 x, mix 0.87 x |

| Direct-path target | Candidate `5eb80fb` direct on | ScyllaDB 6.2.3 (this session) | Result |
| --- | --- | --- | --- |
| Serial update p50 at most 7.6 ms (ScyllaDB in NS11) | 20.6 ms | 9.9 ms | Miss |
| Point-get p50 at most 6.6 ms | 6.3 / 7.7 / 7.0 / 6.4 / 6.2 / 6.1 / 5.2 ms at 1 / 2 / 5 / 20 / 50 / 100 / 200/s | 6.9 / 6.8 / 7.1 / 7.6 / 7.1 / 5.7 / 5.4 ms | Pass at 5 of 7 rates (miss at 2 and 5/s) |
| Serial update p50 at most 15 ms (plan) | 20.6 ms | | Miss |
| Zero rejections through 200 updates/s with p99 at most 25 ms | 0 rejections through 100/s, but p99 29.1 ms already at 1/s and 60–67 ms at 50–100/s; at 200/s 1,041 rejections and 99.5/s acknowledged | Clean through 200/s, p99 24.7 ms | Miss |
| Within 20% of ScyllaDB | Updates 1.6–2.2 x (serial 2.08 x). Gets 0.84–1.13 x at 1–100/s. Mix 0.79–0.91 x at 1–100/s | | Miss for updates, pass for gets and the mix |

Nabedra's `table.update` reads the row before it writes and returns whether the row existed. ScyllaDB does that only through a
lightweight transaction (Paxos); the ScyllaDB numbers use a plain prepared `UPDATE` at QUORUM with `--commitlog-sync=batch`.

### Main (PR #20) against the candidate

Main serves a serial update in 41.2 ms (2.0 x the candidate), updates at 1–20/s in 33–37 ms and gets in 10.8–16.8 ms. Its update
sweep stops after 20/s and its get and mix sweeps after 50/s, each on p99 above 100 ms.

Part of that difference is a product difference, not noise: main's API closes the connection after every response. The driver
opened 312 TLS connections for 300 gets at 20/s on main, against 13 on the candidate (about 4 per phase, all for the driver's
stats fetches). With the 0.35 CPU API, each new connection costs a TLS accept of 4.6–5.2 ms at the API (`api_tls_accept` in the candidate's serial runs; PERF2c, PERF2e).

### Effect of deferring the verification snapshot

| Run | p50 | w1 CPU in the measured windows |
| --- | ---: | ---: |
| Candidate, verification after the last phase | 20.6 ms | 0.097 |
| Candidate, `--snapshot-between-phases` | 24.0 ms | 0.318 |
| Main, verification after the last phase | 41.2 ms | 0.160 |
| Main, `--snapshot-between-phases` | 55.8 ms | 0.327 |

The queued snapshot between phases keeps `w1` busy when the next phase starts. It adds 3.4 ms to the candidate's serial p50 and
14.6 ms to main's. NS11's and PERF1's serial numbers include this cost; the other tables here do not.

## Systems and caps

| System | Binary | Notes |
| --- | --- | --- |
| Candidate | `nabedra-db-5eb80fb`, SHA-256 `f2a27de0…55ea6c`, `nabedra-db 0.1.1 oltp` | `[ci bench]` job of [run 44878990620](https://github.com/zPy52/nabedra-database/actions/runs/44878990620), `rust:1.94.1-bookworm`, glibc 2.36, built in 390 s; `--direct-oltp on` and `off` |
| Main (PR #20) | `nabedra-db-c74c2d4`, SHA-256 `bf370a7e…2015ac2` | OLTP sources equal `1ea4622`, which is main `ec0dd55` apart from Events; ORM tree of `c74c2d4` (CLI 0.2.1); `--direct-oltp on` |
| Baseline | `nabedra-db-b29a178`, SHA-256 `953a35f3…e1012e` | `--pool-format-version 1`, the `b29a178` Python ORM tree |
| ScyllaDB | `scylladb/scylla:6.2.3@sha256:a9d90408…` | RF3, QUORUM, `--commitlog-sync=batch`, `--smp=1`, `--memory=1700M`, driver 3.29.13 |

All Nabedra runs used image `nabedra-oltp-perf-builder:1.94.1` (`sha256:1547483c…`, `TOKIO_WORKER_THREADS=2`). The fsync runs used
`nabedra-bench2-strace:1.94.1`, which is that image plus Debian strace 6.1 ([strace.Dockerfile](strace.Dockerfile)). It was built
locally as benchmark tooling, as in NS11, and removed afterwards. Docker Desktop 28.3.3 ran linuxkit 6.10.14 with 8 vCPUs,
8.22 GB and cgroup v2.

Every Nabedra run verified its caps through Docker inspection and each container's `cpu.max` (`budget_verified: true` in all 22
runs):
- workers: 1 CPU (`100000 100000`), 2.15 GB, swap equal to memory;
- API: 0.35 CPU (`35000 100000`), 550 MB;
- driver: 0.65 CPU, 900 MB.

No run had an OOM kill, an `oom` event or `OOMKilled`. In each ScyllaDB run the script checked three 1 CPU / 2.15 GB nodes with
swap equal to memory, and the same driver cap. The nodes' `memory.events` showed `oom 0` and `oom_kill 0`.

## Driver

Every system used the PERF2e driver: connections are reused while idle under 25 s, and one verification snapshot is read after
the last measured phase. The ScyllaDB driver shares the workload module and its deferred verification. BENCH2 added one option,
`--cold-restart-certificates keep|new`, with `keep` as the default. Its cold restart keeps each worker's key and certificate,
gives the worker a new incarnation and raises its enrollment revision by one, as a restarted container does. GATEfix showed that
new certificates need an API restart; the NS11 driver always minted new ones.

All runs used `--seed 20261118` and `--timeout 10`, except the fsync runs (`--timeout 60`).

| Workload | Arguments |
| --- | --- |
| Serial pair | `--rates 0.2 --seconds 5 --warmup-seconds 5 --repetitions 10 --clients 1 --rows 2000` |
| Serial pair, snapshot between phases | the serial pair plus `--snapshot-between-phases` |
| Update sweep | `--rates 1,2,5,20,50,100,200,500,1000 --seconds 5 --warmup-seconds 3 --repetitions 3 --clients 32 --rows 5000 --stop-p99-ms 100 --stop-on-rejection`; candidate direct on and baseline with `--cold-restart` (after the last phase) |
| Point-get sweep | the update sweep with `--operation get` |
| 95/5 mix | the update sweep with `--operation mix --read-fraction 0.95` |
| fsync run | `--rates 1,5,20 --seconds 5 --warmup-seconds 3 --repetitions 3 --clients 32 --rows 2000 --strace --timeout 60`, no stop rule |
| Storage soak | `--soak --soak-rows 80000 --soak-payload-bytes 2048 --soak-seconds 600 --soak-phase-seconds 60 --soak-rate 50\|100 --clients 32 --soak-verify-keys 500 --cold-restart` |

## Results

Each cell gives acknowledged/offered requests over the measured repetitions, the median acknowledged rate, and the medians of
the per-repetition p50 and p99 in ms, measured from the scheduled arrival. `rej` counts 429 rejections, `cap` counts arrivals
refused by the driver's 32-client limit, and `unres` counts unresolved outcomes. Every run in these tables has
`correctness_verified: true`, except the baseline fsync run.

#### Serial pair

| Offered/s | Candidate direct on | Candidate direct off | Main | ScyllaDB 6.2.3 | b29a178 |
| ---: | --- | --- | --- | --- | --- |
| 0.2 | 10/10 · 0.20/s · p50 20.6 · p99 20.6 | 10/10 · 0.20/s · p50 516.4 · p99 516.4 | 10/10 · 0.20/s · p50 41.2 · p99 41.2 | 10/10 · 0.20/s · p50 9.9 · p99 9.9 | 10/10 · 0.20/s · p50 1,143.5 · p99 1,143.5 |
| 0.2, snapshot between phases | 10/10 · 0.20/s · p50 24.0 · p99 24.0 | | 10/10 · 0.20/s · p50 55.8 · p99 55.8 | | |

Candidate serial details (`perf2e/analyze.py`): p90 26.0 ms, max 31.4 ms. The API's dispatch takes 18.5 ms and the
client-to-dispatch leg 3.1 ms. Each update reuses its connection; the driver's stats fetch opens a new one each phase.

#### Update sweep

Stopped after: candidate on 200 (rejections), candidate off 1, main 20, ScyllaDB 500, baseline 1.

| Offered/s | Candidate direct on | Candidate direct off | Main | ScyllaDB 6.2.3 | b29a178 |
| ---: | --- | --- | --- | --- | --- |
| 1 | 15/15 · 1.00/s · p50 17.6 · p99 29.1 | 15/15 · 1.00/s · p50 506.8 · p99 543.8 | 15/15 · 1.00/s · p50 36.7 · p99 40.1 | 15/15 · 1.00/s · p50 9.5 · p99 10.5 | 15/15 · 0.88/s · p50 2,947.5 · p99 3,512.3 |
| 2 | 30/30 · 2.00/s · p50 20.0 · p99 30.0 | – | 30/30 · 2.00/s · p50 35.6 · p99 42.1 | 30/30 · 2.00/s · p50 9.2 · p99 11.3 | – |
| 5 | 75/75 · 5.00/s · p50 17.0 · p99 31.1 | – | 75/75 · 5.00/s · p50 35.6 · p99 57.3 | 75/75 · 5.00/s · p50 9.5 · p99 14.0 | – |
| 20 | 300/300 · 20.00/s · p50 14.9 · p99 27.2 | – | 300/300 · 20.00/s · p50 33.3 · p99 105.4 | 300/300 · 20.00/s · p50 9.3 · p99 13.6 | – |
| 50 | 750/750 · 49.97/s · p50 14.6 · p99 60.0 | – | – | 750/750 · 49.93/s · p50 8.6 · p99 19.9 | – |
| 100 | 1500/1500 · 99.80/s · p50 15.7 · p99 67.0 | – | – | 1500/1500 · 99.96/s · p50 7.4 · p99 14.9 | – |
| 200 | 1573/3000 · 99.49/s · p50 148.9 · p99 724.7, rej 1041, cap 386 | – | – | 3000/3000 · 200.00/s · p50 6.1 · p99 24.7 | – |
| 500 | – | – | – | 6817/7500 · 440.22/s · p50 26.9 · p99 191.2, cap 683 | – |

At 200 updates/s the candidate acknowledges 99.5/s. The API returns 429 `executor admission capacity exhausted` (1,041 of 3,000).
The driver used 0.55 of its 0.65 CPU in those phases, against 0.16 at 100/s. The same driver reached 198 gets/s, so the update
plateau near 100/s is the server's.

#### Point-get sweep

Stopped after: candidate on 200 (rejections), candidate off 1, main 50, ScyllaDB 500, baseline 1.

| Offered/s | Candidate direct on | Candidate direct off | Main | ScyllaDB 6.2.3 | b29a178 |
| ---: | --- | --- | --- | --- | --- |
| 1 | 15/15 · 1.00/s · p50 6.3 · p99 9.4 | 15/15 · 1.00/s · p50 161.2 · p99 198.0 | 15/15 · 1.00/s · p50 10.8 · p99 20.7 | 15/15 · 1.00/s · p50 6.9 · p99 7.9 | 15/15 · 1.00/s · p50 514.9 · p99 685.0 |
| 2 | 30/30 · 2.00/s · p50 7.7 · p99 9.5 | – | 30/30 · 2.00/s · p50 16.8 · p99 26.3 | 30/30 · 2.00/s · p50 6.8 · p99 8.7 | – |
| 5 | 75/75 · 5.00/s · p50 7.0 · p99 13.1 | – | 75/75 · 5.00/s · p50 14.8 · p99 22.1 | 75/75 · 5.00/s · p50 7.1 · p99 10.4 | – |
| 20 | 300/300 · 20.00/s · p50 6.4 · p99 12.5 | – | 300/300 · 20.00/s · p50 13.5 · p99 22.1 | 300/300 · 20.00/s · p50 7.6 · p99 11.3 | – |
| 50 | 750/750 · 50.00/s · p50 6.2 · p99 11.5 | – | 750/750 · 50.00/s · p50 12.3 · p99 120.4 | 750/750 · 50.00/s · p50 7.1 · p99 12.1 | – |
| 100 | 1500/1500 · 99.95/s · p50 6.1 · p99 15.1 | – | – | 1500/1500 · 99.98/s · p50 5.7 · p99 13.7 | – |
| 200 | 2944/3000 · 198.07/s · p50 5.2 · p99 127.7, rej 36, cap 20 | – | – | 3000/3000 · 200.00/s · p50 5.4 · p99 27.8 | – |
| 500 | – | – | – | 6993/7500 · 467.75/s · p50 31.2 · p99 112.7, cap 507 | – |

#### 95/5 mix (all operations; reads and writes separately in `runs/mix-*.json`)

Stopped after: candidate on 200 (rejections), candidate off 1, main 50, ScyllaDB 500, baseline 1.

| Offered/s | Candidate direct on | Candidate direct off | Main | ScyllaDB 6.2.3 | b29a178 |
| ---: | --- | --- | --- | --- | --- |
| 1 | 15/15 · 1.00/s · p50 7.1 · p99 10.7 | 15/15 · 1.00/s · p50 162.4 · p99 382.9 | 15/15 · 1.00/s · p50 13.3 · p99 22.6 | 15/15 · 1.00/s · p50 7.8 · p99 9.6 | 15/15 · 1.00/s · p50 572.5 · p99 1,009.8 |
| 2 | 30/30 · 2.00/s · p50 6.5 · p99 15.8 | – | 30/30 · 2.00/s · p50 16.7 · p99 40.2 | 30/30 · 2.00/s · p50 7.4 · p99 9.7 | – |
| 5 | 75/75 · 5.00/s · p50 6.2 · p99 18.8 | – | 75/75 · 5.00/s · p50 17.5 · p99 29.4 | 75/75 · 5.00/s · p50 7.8 · p99 9.9 | – |
| 20 | 300/300 · 20.00/s · p50 6.9 · p99 24.3 | – | 300/300 · 20.00/s · p50 15.6 · p99 66.7 | 300/300 · 20.00/s · p50 7.8 · p99 10.8 | – |
| 50 | 750/750 · 50.00/s · p50 6.5 · p99 23.0 | – | 750/750 · 49.98/s · p50 12.6 · p99 113.0 | 750/750 · 50.00/s · p50 7.3 · p99 17.4 | – |
| 100 | 1500/1500 · 99.99/s · p50 5.7 · p99 21.5 | – | – | 1500/1500 · 99.95/s · p50 6.7 · p99 20.9 | – |
| 200 | 2893/3000 · 198.07/s · p50 5.6 · p99 156.8, rej 65, cap 42 | – | – | 3000/3000 · 200.00/s · p50 5.7 · p99 24.3 | – |
| 500 | – | – | – | 7012/7500 · 465.41/s · p50 32.0 · p99 113.1, cap 488 | – |

#### fsync run (strace on every worker; latency distorted by strace)

| Offered/s | Candidate direct on | b29a178 |
| ---: | --- | --- |
| 1 | 15/15 · 1.00/s · p50 41.7 · p99 59.0 | 9/15 · 0.12/s · p50 20,473.0 · p99 24,418.7, rej 4, unres 2 |
| 5 | 75/75 · 5.00/s · p50 41.5 · p99 78.0 | 10/75 · 0.03/s · p50 38,965.6 · p99 56,618.4, rej 54, unres 11 |
| 20 | 300/300 · 20.00/s · p50 64.4 · p99 230.0 | 10/300 · 0.09/s · p50 25,761.2 · p99 33,363.5, rej 286, unres 4 |

| Rate | Candidate syncs / ack | Candidate wbytes / ack | b29a178 syncs / ack | b29a178 wbytes / ack |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 3.00 (45 / 15) | 21.6 KB | 226.9 (2,042 / 9) | 1,250 KB |
| 5 | 3.04 (228 / 75) | 25.3 KB | 485.9 (4,859 / 10) | 2,771 KB |
| 20 | 2.98 (893 / 300) | 14.8 KB | 284.6 (2,846 / 10) | 3,862 KB |

The candidate issues only `fdatasync`: 465–469 per worker over the run, p50 1.17–1.34 ms and p99 6.5–9.3 ms. The baseline's
final verification snapshot (`query.collect` of 2,000 rows) timed out at 60 s under strace. Its phases and counts are complete.

## G6: cold restart

| | Candidate after update sweep | Candidate after soak 50/s | Candidate after soak 100/s | b29a178 after update sweep |
| --- | ---: | ---: | ---: | ---: |
| Data per worker | (5,000 rows) | 1.10 GB | 1.23 GB | (5,000 rows) |
| All workers ready on the new run | 6.0 s | 5.8 s | 7.0 s | 6.4 s |
| `data.direct.v1` active again | 6.2 s | 5.9 s | 7.2 s | – |
| First point get through the running API | 6.5 s (1 attempt) | 6.1 s | 7.3 s | none in 180 s: 503 `storage_unavailable` "worker transport unavailable", outcome unknown |

Each restart SIGKILLs all three workers, then starts them with a cold profile onto a new run, keeping their certificates. The
time runs from the first `docker start`. An in-place restart of one SIGKILLed worker is refused by design (GATEfix), so it was
not run.

## G7: worker memory

Largest `memory.peak` of any worker per run, in MB. It is the maximum over the 5 s cgroup samples, the sample before a cold
restart's kill and the final read. Anon and page cache (`file`) are the maxima of `memory.stat` on that worker, in MiB.

| Workload | Candidate direct on | Candidate direct off | Main | b29a178 |
| --- | --- | --- | --- | --- |
| Serial | 70.9 (w1; anon 37, file 26) | 81.4 | 149.8 (w1; anon 36, file 90) | 92.7 (w1; anon 44, file 40) |
| Update sweep | 126.0 (w1; anon 60, file 55; ran to 200/s) | 98.7 | 152.0 | 102.1 (stopped at 1/s) |
| Get sweep | 78.8 | 91.1 | 145.9 | 97.0 |
| Mix | 84.9 | 102.6 | 154.5 | 97.5 |
| fsync run | 73.0 (anon 39, file 25) | | | 108.6 (w1; anon 63, file 37) |
| Soak 50/s | 1,595 (anon 102, file 1,268) | | | not run |
| Soak 100/s | 1,728 (anon 102, file 1,275) | | | not run |

- **Main's page cache.** Main's 72–93 MiB of `file` per worker is the zero-filled 64 MiB journal segment. The candidate drops
  those pages (GATEfix), and its serial peak falls from 149.8 to 70.9 MB.
- **What remains.** The rest of the candidate's peak scales with load. Its leader's anon memory is at most 60 MiB, against 63
  MiB on the baseline. Transparent huge pages back 20–32 MiB of it.
- **Soaks.** The soak peaks are page cache over 1.1–1.2 GB of runs, well under the 2.15 GB limit, with no OOM. There is no
  baseline soak to compare: NS11 could not seed it.

## Storage soak

| | 50 updates/s | 100 updates/s |
| --- | --- | --- |
| Seeding, 80,000 x 2 KiB rows, 4 clients | 46.6 s, no rejections | 47.7 s, no rejections |
| Acknowledged / offered over 10 x 60 s | 28,505 / 30,000 (rej 1,235, cap 260) | 53,685 / 60,000 (rej 5,890, cap 425) |
| Phases without rejections | 3 of 10 | 0 of 10 |
| Phase p50 | 16.1–18.7 ms | 22.8–45.6 ms |
| Phase p99 | 59 to 2,443 ms | 234 to 475 ms |
| Phase max | 95 ms to 9.1 s | 3.5 to 7.0 s |
| Verification, 500 sampled keys | 0 mismatches | 0 mismatches |
| Write amplification per worker (flush + compaction + journal over user bytes) | (262 + 1,377 + 595) MB / 526 MB = 4.2 x | (331 + 2,118 + 801) MB / 672 MB = 4.8 x |
| `stall_ms` per worker | 116–270 | 21–68 |
| Refusals | all 429 `executor admission capacity exhausted` | same |

The highest clean rate of the update sweep is 100/s, and NS11 soaked at 50/s, so both rates ran. Neither soak holds its rate
cleanly once flush and compaction run. At 50/s, 4.1% of updates were rejected, against 3.7–4.5% for `20747f8` in NS11. At
100/s, 9.8% were rejected. The API container was CFS-throttled in 520 of 6,655 periods. No direct fallbacks, retries or proposal
failures were recorded, and every entry carried one row (`batch_sizes: {le_1}`).

## Host load

The Docker lane was held from 07:14:48Z to 09:25:15Z. Before every run, the script waited until the 1-minute load was below 3
(`waited`, in s). `load.log` has every start and end. Docker Desktop's VM accounted for most of the load during the runs. In the two process checks I made between runs, macOS
`syspolicyd` used up to one core, and no `cargo` or `rustc` process was running.

| Run | Start (UTC) | Waited s | Load at start (1 / 5 / 15 min) | Seconds | Load at end | rc |
| --- | --- | ---: | --- | ---: | --- | ---: |
| serial-main | 07:16:34 | 90 | 2.49 / 3.38 / 3.27 | 88 | 3.27 / 3.39 / 3.28 | 0 |
| serial-scylla | 07:18:59 | 45 | 2.90 / 3.30 / 3.25 | 126 | 3.18 / 3.28 / 3.25 | 0 |
| serial-base | 07:22:05 | 60 | 2.88 / 3.20 / 3.22 | 118 | 5.17 / 4.25 / 3.66 | 0 |
| serial-cand-on | 07:25:19 | 45 | 2.93 / 3.77 / 3.53 | 90 | 4.81 / 4.12 / 3.68 | 0 |
| serial-cand-off | 07:27:49 | 60 | 2.68 / 3.64 / 3.52 | 98 | 4.85 / 4.14 / 3.73 | 0 |
| serial-snap-cand-on | 07:30:41 | 60 | 2.84 / 3.73 / 3.61 | 133 | 6.02 / 4.75 / 4.04 | 0 |
| serial-snap-main | 07:35:39 | 165 | 2.89 / 3.99 / 3.85 | 127 | 4.95 / 4.43 / 4.05 | 0 |
| update-cr-cand-on | 07:40:17 | 150 | 2.44 / 3.83 / 3.88 | 201 | 5.98 / 4.67 / 4.20 | 0 |
| update-cand-off | 07:45:39 | 120 | 2.44 / 3.87 / 3.95 | 79 | 4.89 / 4.21 / 4.06 | 0 |
| update-main | 07:48:13 | 75 | 2.96 / 3.79 / 3.91 | 116 | 4.88 / 4.08 / 4.00 | 0 |
| update-scylla | 07:50:54 | 45 | 2.98 / 3.70 / 3.87 | 213 | 3.91 / 3.89 / 3.92 | 0 |
| update-cr-base | 07:56:13 | 105 | 2.84 / 3.59 / 3.80 | 314 | 3.12 / 3.68 / 3.82 | 0 |
| get-cand-on | 08:02:12 | 45 | 2.79 / 3.52 / 3.76 | 188 | 4.34 / 3.93 / 3.87 | 0 |
| get-cand-off | 08:05:50 | 30 | 2.90 / 3.62 / 3.76 | 80 | 3.88 / 3.73 / 3.78 | 0 |
| get-main | 08:07:56 | 45 | 2.68 / 3.45 / 3.68 | 136 | 4.06 / 3.61 / 3.70 | 0 |
| get-scylla | 08:10:43 | 30 | 2.81 / 3.35 / 3.60 | 213 | 3.65 / 3.68 / 3.70 | 0 |
| get-base | 08:15:01 | 45 | 2.90 / 3.48 / 3.62 | 128 | 6.71 / 4.44 / 3.96 | 0 |
| mix-cand-on | 08:20:09 | 180 | 2.83 / 3.86 / 3.82 | 189 | 4.76 / 4.37 / 4.05 | 0 |
| mix-cand-off | 08:24:03 | 45 | 2.71 / 3.89 / 3.89 | 79 | 5.12 / 4.20 / 3.99 | 0 |
| mix-main | 08:26:38 | 75 | 2.59 / 3.63 / 3.79 | 136 | 2.75 / 3.47 / 3.72 | 0 |
| mix-scylla | 08:28:54 | 0 | 2.75 / 3.47 / 3.72 | 214 | 4.26 / 3.87 / 3.85 | 0 |
| mix-base | 08:32:59 | 30 | 2.87 / 3.56 / 3.73 | 122 | 6.47 / 4.63 / 4.14 | 0 |
| fsync-cand-on | 08:36:32 | 75 | 2.80 / 3.86 / 3.89 | 125 | 4.77 / 4.25 / 4.04 | 0 |
| fsync-base | 08:39:52 | 75 | 2.93 / 3.76 / 3.86 | 995 | 7.77 / 9.02 / 7.55 | 1 (final snapshot timeout) |
| soak-cand-on | 08:58:27 | 120 | 2.68 / 6.80 / 6.87 | 693 | 6.48 / 6.09 / 6.32 | 0 |
| soak100-cand-on | 09:12:32 | 120 | 2.73 / 4.68 / 5.70 | 687 | 5.79 / 6.16 / 6.07 | 0 |

## Files

- `runs/<workload>-<system>.json`: compact records (`cand-on`, `cand-off`, `main`, `base`, `scylla`). Each has the settings,
  binary SHA-256, image, Docker environment, per-rate summary, deferred verification, direct counters, client-observed
  refusals, cgroup I/O and CPU, strace counts, worker memory, caps inspection and the cold-restart or soak record. `update-cr-*`
  are the update sweeps followed by a cold restart.
- `memory-and-caps.json`: the G7 table and per-container caps for every Nabedra run (`gstats.py`).
- `fsync-rates.txt`: G4 and G5 per rate (`fsync_rates.py`).
- `storage-soak.json`: each worker's `nabedra-kv` storage counters at the end of both soaks.
- `load.log`, `run.sh`, `collect.py` (with NS11's `analyze.py` and `summarize.py`), `strace.Dockerfile`.
- Cells come from `../summary.py`, serial details from `../perf2e/analyze.py`.
- The full outputs (per-request samples, worker logs, cgroup samples, strace logs, about 120 MB) are in the ledger under
  `.nabedra/workflows/remove-rocksdb/bench2/runs/`.
