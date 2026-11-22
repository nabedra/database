# Final comparative benchmark of the hardening PR (BENCH3)

Measured on 2026-11-22 between 09:49Z and 11:54Z on the controlled host (this Mac), one system at a time. Runs of the
candidate, main and ScyllaDB alternate within each workload. The candidate is `nabedra/hardening` `c38a4a7`. The base is
`main` `6141321` (PR #23 rr-latency plus the CI port). ScyllaDB 6.2.3 uses BENCH2's setup.

**Storage.** Docker Desktop's disk is on the external USB SSD `/Volumes/ZX20` (since 2026-11-21 ~13:45 CET). These
numbers are therefore not comparable with BENCH2 ([../final/README.md](../final/README.md)) or NS11, which ran on the
internal disk. Every comparison below is within this session.

## Verdict

- **Correctness (G1).** All 9 candidate runs verified, with 0 mismatches and 0 unresolved outcomes. Main failed G1 in one
  run: the first 100/s soak left 18 unresolved updates (client timeouts after 10 s), although its 500 sampled keys had 0
  mismatches.
- **Writes.**
  - The candidate's serial update p50 is 16.5 / 17.6 ms, against 18.4 / 22.5 ms for main and 8.5 / 6.4 ms for ScyllaDB.
  - In the update sweep the candidate is 2.7–7.1 ms faster than main at every rate both ran. It has no server refusals
    through 300/s, where the sweep stopped on the 100 ms p99 rule (289 acknowledged/s).
  - Main stopped at 100/s on p99 (106.6 ms).
  - ScyllaDB stays clean through 300/s at p50 5.7 ms.
- **Sustained writes.** The candidate holds both soaks with 0 refusals: 50/s, and 100/s twice.
  - Main refuses 3.4% at 50/s and 9.1% / 25.6% in its two 100/s soaks.
  - The candidate's 100/s soak p99 is 183–198 ms, so its tail under the 2 KiB-row soak is still long.
- **Reads.** Get p50 is 4.4–5.6 ms through 300/s, at or below ScyllaDB's 4.8–7.3 ms at every rate. Main refuses 17 gets at
  200/s. The 95/5 mix is clean through 200/s on all three systems; at 300/s main refuses 144 and the candidate none.
- **DirectBatch.** It forms through the real API: 1.13 commits per Data entry at 100/s, 1.46 at 200/s and 2.38 at 300/s
  (up to 32 members). The leader's journal syncs per write fall from 1.00 to 0.42.
- **Recovery (G6).** A cold restart of all three workers keeps their certificates. The running API serves again 5.6–7.2 s
  after the restart, against 5.9–8.4 s for main.
- **Memory (G7).** At equal load the candidate is at or below main: through 100/s, 101.4 vs 105.1 MB (updates), 80.3 vs
  80.3 MB (gets) and 75.9 vs 81.1 MB (mix). Its whole-run peak in the update sweep is higher (141.8 vs 116.2 MB) because it
  ran 200 and 300/s, which main never reached. In the soaks it is lower (863–967 vs 1,273–1,366 MB). No container had an
  OOM.

## Summary for the PR description

| | Candidate `c38a4a7` | main `6141321` | ScyllaDB 6.2.3 |
| --- | --- | --- | --- |
| Serial update p50 (2 runs) | **16.5 / 17.6 ms** | 18.4 / 22.5 ms | 8.5 / 6.4 ms |
| Serial get p50 | **5.6 ms** | 7.8 ms | 5.9 ms |
| Update sweep: highest rate with every arrival acknowledged | 100/s (200/s: 2,998 / 3,000, 2 driver-capped, 0 refused) | 100/s | 300/s |
| Update sweep: highest rate with 0 server refusals | 300/s, 289 acked/s, p50 23.5, p99 174 ms (stopped on p99) | 100/s, p99 106.6 ms (stopped on p99) | 600/s (491 acked/s, driver-capped) |
| Update p50 / p99 at 100/s | 11.9 / 34.0 ms | 19.0 / 106.6 ms | 6.6 / 16.0 ms |
| Soak 50/s, 5 min: refused, median phase p99 | **0**, 45.5 ms | 516 (3.4%), 148.6 ms | not run (no ScyllaDB soak driver) |
| Soak 100/s, 5 min (2 runs): refused, median phase p99 | **0 / 0**, 197.7 / 182.7 ms | 7,679 (25.6%) + 18 unresolved / 2,730 (9.1%), 1,118 / 511 ms | not run |
| G1 (0 mismatches, 0 unresolved) | Pass in all 9 runs | Fails in 1 of 9 (soak 100/s run 1: 18 unresolved) | 0 mismatches in all 6 runs |
| G6: cold restart, all workers ready / API serves | 4.7 / 5.8 s (update sweep); 5.3–6.8 / 5.6–7.2 s (soaks) | 8.0 / 8.4 s; 5.5–7.8 / 5.9–8.2 s | – |
| G7: largest worker `memory.peak` at equal load, OOM | 101.4 MB updates through 100/s, 80.3 gets, 75.9 mix; soaks 863–967 MB; 0 OOM | 105.1, 80.3, 81.1; soaks 1,273–1,366 MB; 0 OOM | – |
| Leader journal syncs per write (update sweep) | 1.00 → 0.42 as batching grows (100 → 300/s) | 1.00 | – |
| Commits per Data entry (DirectBatch) | 1.13 / 1.46 / 2.38 at 100 / 200 / 300/s | 1.00 (no DirectBatch) | – |
| w1 (leader) CPU per acknowledged update at 100/s | 4.82 ms | 7.65 ms | – |

## Systems and caps

| System | Binary | Notes |
| --- | --- | --- |
| Candidate | `nabedra-db-c38a4a7`, SHA-256 `426516e8…7011d2`, `nabedra-db 0.1.1 oltp` | `bench` job of [run 45008607422](https://github.com/zPy52/nabedra-database/actions/runs/45008607422) (request `bench3-c38a4a7-093930`, branch `nabedra/hd-bench3`), `rust:1.94.1-bookworm`, glibc 2.36, 475 s; `--direct-oltp on`. `data.direct.batch` is enabled by the workers themselves |
| main | `nabedra-db-6141321`, SHA-256 `107127bd…59f8f5` | `bench` job of [run 45008609476](https://github.com/zPy52/nabedra-database/actions/runs/45008609476) (request `bench3-6141321-093930`, dispatched on `main`), 433 s; `--direct-oltp on` |
| ScyllaDB | `scylladb/scylla:6.2.3@sha256:a9d90408…` | RF3, QUORUM, `--commitlog-sync=batch`, `--smp=1`, `--memory=1700M`, driver 3.29.13 |

- **Image and ORM.** Both Nabedra binaries ran in image `nabedra-oltp-perf-builder:1.94.1` (`sha256:1547483c…`,
  `TOKIO_WORKER_THREADS=2`) with the candidate's benchmark driver and Python ORM tree. `orm/` is identical between
  `6141321` and `c38a4a7`. The driver's changes since `6141321` (`--api-cpus`, `--hold-compaction`, `--worker-env`) default
  to the old behaviour and were not used.
- **Host.** Docker Desktop 28.3.3, linuxkit 6.10.14, 8 vCPU, 8.22 GB, cgroup v2.
- **Caps (NS11).** Every Nabedra run had `budget_verified: true`:
  - workers: 1 CPU, 2.15 GB, swap = memory;
  - API: 0.35 CPU, 550 MB;
  - driver: 0.65 CPU, 900 MB;
  - 4 CPU / 7.9 GB in total.
- **ScyllaDB caps.** In every ScyllaDB run the compose check passed: three 1 CPU / 2.15 GB nodes and the same driver cap.
- **OOM.** No container in any run had `OOMKilled`, an `oom` event or an `oom_kill`.

## Method

BENCH2's settings at the NS11 caps. All runs used `--seed 20261118 --timeout 10`.

| Workload | Arguments |
| --- | --- |
| Serial update | `--rates 0.2 --seconds 5 --warmup-seconds 5 --repetitions 10 --clients 1 --rows 2000` |
| Serial get | the serial update with `--operation get` |
| Update sweep | `--rates 1,5,20,50,100,200,300,400,600 --seconds 5 --warmup-seconds 3 --repetitions 3 --clients 32 --rows 5000 --stop-p99-ms 100 --stop-on-rejection`; Nabedra with `--cold-restart` (G6) |
| Get sweep | the update sweep with `--operation get`, no cold restart |
| 95/5 mix | the update sweep with `--operation mix --read-fraction 0.95`, no cold restart |
| Soak | `--soak --soak-rows 80000 --soak-payload-bytes 2048 --soak-seconds 300 --soak-phase-seconds 60 --soak-rate 50\|100 --clients 32 --soak-verify-keys 500 --cold-restart` |

- **Order.** Each workload ran each system once, in a rotated order:
  - serial (cand, main, scylla);
  - serial get (main, scylla, cand);
  - serial again (scylla, cand, main);
  - update (cand, main, scylla);
  - get (main, scylla, cand);
  - mix (scylla, cand, main);
  - soak 50 (cand, main);
  - soak 100 (main, cand);
  - soak 100 again (main, cand). This repeat ran because the first main run failed G1 while the host load reached 13.
- **Windows.** There were four lane windows of 45, 26, 36 and 14 minutes, and the lane was released between them.
- **Load.** Before every run, `run.sh` waited until the 1-minute load was below 3. Waits were 0–345 s and starting loads
  2.20–2.98. Docker Desktop's VM raised the load to 2.9–13.0 during runs.
  - During the repeated soaks `w4-top.log` sampled the top processes every 60 s. Docker's VM used 240–385% CPU and
    nothing else over 100%. No `cargo` or `rustc` ran.
  - The first 100/s soak of main is the only run whose end load (13.0) stands out; no process sample exists for it.
- **Stop rules.** The sweeps stop after the first rate whose median p99 exceeds 100 ms or that has a 429. `cap` counts
  arrivals refused by the driver because all 32 clients were busy. They are not server refusals, but they do mean the
  server was not keeping up at that rate.
- **Changes.** No benchmark settings or product code were changed.
- **Fsyncs.** Fsyncs per write come from each worker's `storage.journal_syncs` counter in `/v1/runtime/stats`, not from
  strace. So latencies are undistorted, and there are no strace runs.

## Results

Cells: acknowledged/offered over the measured repetitions · median acknowledged rate · medians of the per-repetition p50
and p99 in ms from the scheduled arrival; `rej` = 429 refusals, `cap` = driver-capped arrivals, `unres` = unresolved.
Every run in these tables has `correctness_verified: true`.

### Serial

| | Candidate | main | ScyllaDB |
| --- | --- | --- | --- |
| Update, run 1 | 10/10 · p50 16.5 · p90 23.1 · max 36.8 | 10/10 · p50 18.4 · p90 30.2 · max 33.3 | 10/10 · p50 8.5 |
| Update, run 2 | 10/10 · p50 17.6 · p90 22.3 · max 25.2 | 10/10 · p50 22.5 · p90 29.0 · max 43.7 | 10/10 · p50 6.4 |
| Get | 10/10 · p50 5.6 · p90 8.1 | 10/10 · p50 7.8 · p90 11.2 | 10/10 · p50 5.9 |

Stage means over the serial updates (ms, `rd3/analyze.py --stages`):

| Stage | Candidate run 1 / 2 | main run 1 / 2 |
| --- | --- | --- |
| API `http.dispatch.total` | 14.5 / 14.1 | 17.8 / 19.9 |
| leader `leader.mutation.total` | 13.3 / 12.6 | 14.5 / 15.9 |
| leader `leader.attempt.timestamp_fetch` | 1.10 / 1.22 | 3.73 / 5.30 |
| leader `raft.data.client_write` | 11.6 / 10.6 | 10.3 / 10.0 |
| leader `raft.data.append_encode` | 0.015 / 0.015 | 0.67 / 0.50 |
| follower `raft.data.net.append_handle` | 8.3 / 7.6 | 5.0 / 4.7 |
| of which follower `raft.data.append_write` (synced journal write) | 7.9 / 7.3 | 4.3 / 4.0 |

The candidate's Timestamp leg (RD2) and the leader's encode (RD3) are cheaper. Its serial `client_write` is not, because
the follower's synced journal write takes 7.3–7.9 ms against 4.0–4.3 ms on main in these serial runs, one write every 5 s.
In the update sweep the same stage is 1.9–3.0 ms on both systems and the candidate's `client_write` is 5.3–7.0 ms against
main's 8.3–9.4 ms. So the serial gap looks like the USB SSD's cost for an fsync after an idle period, not a code path. It
is not proven: there are two serial runs per system.

### Update sweep

Stopped after: candidate 300 (p99 174 ms), main 100 (p99 106.6 ms), ScyllaDB 600 (end of the rate list).

| Offered/s | Candidate | main | ScyllaDB 6.2.3 |
| ---: | --- | --- | --- |
| 1 | 15/15 · 1.00/s · p50 13.4 · p99 17.3 | 15/15 · 1.00/s · p50 17.8 · p99 23.3 | 15/15 · 1.00/s · p50 9.6 · p99 12.9 |
| 5 | 75/75 · 5.00/s · p50 13.1 · p99 29.8 | 75/75 · 5.00/s · p50 19.3 · p99 26.7 | 75/75 · 5.00/s · p50 9.1 · p99 13.4 |
| 20 | 300/300 · 20.00/s · p50 11.8 · p99 55.2 | 300/300 · 20.00/s · p50 15.7 · p99 34.1 | 300/300 · 20.00/s · p50 7.7 · p99 11.6 |
| 50 | 750/750 · 49.94/s · p50 11.7 · p99 28.5 | 750/750 · 49.91/s · p50 14.4 · p99 43.3 | 750/750 · 50.00/s · p50 7.5 · p99 15.1 |
| 100 | 1500/1500 · 99.90/s · p50 11.9 · p99 34.0 | 1500/1500 · 99.47/s · p50 19.0 · p99 106.6 | 1500/1500 · 99.93/s · p50 6.6 · p99 16.0 |
| 200 | 2998/3000 · 199.52/s · p50 13.7 · p99 73.9, cap 2 | – | 3000/3000 · 200.00/s · p50 5.5 · p99 20.2 |
| 300 | 4364/4500 · 289.42/s · p50 23.5 · p99 174.0, cap 136 | – | 4500/4500 · 299.78/s · p50 5.7 · p99 33.1 |
| 400 | – | – | 5945/6000 · 395.55/s · p50 6.6 · p99 76.8, cap 55 |
| 600 | – | – | 6898/9000 · 491.26/s · p50 51.3 · p99 160.1, cap 2102 |

Per rate, the candidate's resources (`rates.txt`):
- CPU is in ms per acknowledged update.
- Journal syncs per write are summed over the three workers, with the leader in brackets.
- Bytes are cgroup `io.stat` write bytes per write over the three workers.

| Offered/s | Cand CPU w1 / w2 / w3 / API | main CPU w1 / w2 / w3 / API | Cand syncs (w1) | main syncs (w1) | Cand bytes | main bytes | Cand commits per entry, members |
| ---: | --- | --- | --- | --- | ---: | ---: | --- |
| 20 | 10.60 / 7.44 / 7.21 / 2.13 | 13.66 / 8.61 / 7.62 / 2.03 | 3.00 (1.00) | 3.01 (1.00) | 23.1 KB | 30.0 KB | 1.00 |
| 50 | 6.59 / 4.75 / 4.49 / 1.32 | 9.56 / 6.19 / 5.37 / 1.42 | 2.86 (0.95) | 3.00 (1.00) | 21.3 KB | 25.8 KB | 1.05; 34 batches of 2–3 |
| 100 | 4.82 / 3.40 / 3.10 / 0.93 | 7.65 / 4.79 / 4.25 / 1.14 | 2.67 (0.89) | 2.97 (1.00) | 22.7 KB | 29.9 KB | 1.13; up to 8 |
| 200 | 3.45 / 2.38 / 2.18 / 0.83 | – | 2.05 (0.68) | – | 12.9 KB | – | 1.46; 532 batches, up to 16 |
| 300 | 2.29 / 1.46 / 1.29 / 0.64 | – | 1.26 (0.42) | – | 55.9 KB (a flush or compaction fell in the window) | – | 2.38; 972 batches, up to 32 |

At 1 and 5/s the CPU per ack is dominated by idle work (heartbeats, Control) and is not a cost per write. ScyllaDB's
nodes report only whole-run cgroup totals. Over its update sweep they used 3.99 ms of CPU and 84.9 KB of writes per
acknowledged update, startup and warm-ups included, so these figures are not comparable cell by cell. The direct
counters (`fallbacks`, `retries`, `proposal_failures`, summed over the workers) are 0 in every Nabedra run except main's
first 100/s soak: 4 retries and 1 proposal failure.

### Point-get sweep

Stopped after: candidate 400 (p99 207 ms), main 200 (17 refusals), ScyllaDB 600.

| Offered/s | Candidate | main | ScyllaDB 6.2.3 |
| ---: | --- | --- | --- |
| 1 | 15/15 · 1.00/s · p50 4.4 · p99 7.3 | 15/15 · 1.00/s · p50 6.6 · p99 9.5 | 15/15 · 1.00/s · p50 5.2 · p99 8.4 |
| 5 | 75/75 · 5.00/s · p50 5.3 · p99 9.9 | 75/75 · 5.00/s · p50 6.1 · p99 11.5 | 75/75 · 5.00/s · p50 7.3 · p99 10.0 |
| 20 | 300/300 · 20.00/s · p50 4.5 · p99 51.2 | 300/300 · 20.00/s · p50 5.5 · p99 9.7 | 300/300 · 20.00/s · p50 6.5 · p99 10.7 |
| 50 | 750/750 · 50.00/s · p50 5.6 · p99 11.1 | 750/750 · 50.00/s · p50 5.5 · p99 42.9 | 750/750 · 50.00/s · p50 6.3 · p99 12.5 |
| 100 | 1500/1500 · 99.97/s · p50 5.1 · p99 15.0 | 1500/1500 · 99.98/s · p50 5.0 · p99 44.7 | 1500/1500 · 99.98/s · p50 5.9 · p99 13.3 |
| 200 | 3000/3000 · 200.00/s · p50 4.6 · p99 79.8 | 2983/3000 · 199.20/s · p50 5.0 · p99 75.0, rej 17 | 3000/3000 · 200.00/s · p50 5.0 · p99 22.6 |
| 300 | 4475/4500 · 299.57/s · p50 4.5 · p99 85.6, cap 25 | – | 4500/4500 · 299.77/s · p50 4.8 · p99 28.2 |
| 400 | 4671/6000 · 293.58/s · p50 87.2 · p99 207.1, cap 1329 | – | 5988/6000 · 397.77/s · p50 5.7 · p99 77.6, cap 12 |
| 600 | – | – | 7533/9000 · 496.64/s · p50 46.6 · p99 133.3, cap 1467 |

Gets plateau at about 294/s on the candidate, the same ceiling as updates. w1's CPU per get is 0.81 ms at 300 and
400/s, and the API's 0.62 ms per get at 0.35 CPU allows about 560/s. So neither the leader's nor the API's CPU quota is
the limit here; the driver (0.65 CPU) or the API's dispatch are the candidates, not measured. Main's 17 refusals at 200/s
are 429 `executor admission capacity exhausted`.

### 95/5 mix (all operations; reads and writes separately in `runs/mix-*.json`)

Stopped after: candidate 300 (p99 126 ms), main 300 (144 refusals), ScyllaDB 600.

| Offered/s | Candidate | main | ScyllaDB 6.2.3 |
| ---: | --- | --- | --- |
| 1 | 15/15 · 1.00/s · p50 6.0 · p99 8.9 | 15/15 · 1.00/s · p50 6.0 · p99 9.7 | 15/15 · 1.00/s · p50 4.3 · p99 7.6 |
| 5 | 75/75 · 5.00/s · p50 5.9 · p99 22.9 | 75/75 · 5.00/s · p50 6.5 · p99 17.5 | 75/75 · 5.00/s · p50 7.8 · p99 10.8 |
| 20 | 300/300 · 20.00/s · p50 5.9 · p99 17.8 | 300/300 · 20.00/s · p50 6.5 · p99 21.2 | 300/300 · 20.00/s · p50 6.2 · p99 10.6 |
| 50 | 750/750 · 50.00/s · p50 5.7 · p99 26.3 | 750/750 · 50.00/s · p50 5.3 · p99 20.2 | 750/750 · 50.00/s · p50 6.5 · p99 13.5 |
| 100 | 1500/1500 · 99.98/s · p50 5.2 · p99 27.7 | 1500/1500 · 99.98/s · p50 5.2 · p99 54.6 | 1500/1500 · 99.94/s · p50 5.8 · p99 14.9 |
| 200 | 3000/3000 · 200.00/s · p50 4.6 · p99 24.5 | 3000/3000 · 200.00/s · p50 4.7 · p99 16.9 | 3000/3000 · 200.00/s · p50 5.2 · p99 14.2 |
| 300 | 4475/4500 · 298.26/s · p50 4.7 · p99 125.8, cap 25 | 3839/4500 · 242.99/s · p50 76.6 · p99 245.4, rej 144, cap 517 | 4500/4500 · 299.86/s · p50 4.7 · p99 38.7 |
| 400 | – | – | 5987/6000 · 397.94/s · p50 5.5 · p99 51.8, cap 13 |
| 600 | – | – | 7639/9000 · 507.19/s · p50 45.9 · p99 108.2, cap 1361 |

Main's 300/s refusals: 150 gets and 14 updates over the three repetitions, all 429 `executor admission capacity exhausted`.

## Storage soak (5 minutes, 80,000 × 2 KiB rows)

| | Candidate 50/s | main 50/s | Candidate 100/s (run 1 / 2) | main 100/s (run 1 / 2) |
| --- | --- | --- | --- | --- |
| Seeding, 4 clients | 45.2 s, no rejections | 43.6 s | 47.6 / 44.1 s | 48.8 / 44.5 s |
| Acknowledged / offered | 14,997 / 15,000 (cap 3) | 14,383 / 15,000 (rej 516, cap 101) | 29,932 / 30,000 (cap 68); 29,914 / 30,000 (cap 86) | 19,892 / 30,000 (rej 7,679, cap 2,411, unres 18); 27,235 / 30,000 (rej 2,730, cap 35) |
| Phases without refusals | 5 of 5 | 1 of 5 | 5 of 5; 5 of 5 | 0 of 5; 0 of 5 |
| Phase p50 | 12.4–13.2 ms | 15.2–16.8 ms | 12.6–14.9; 13.1–14.3 ms | 68–205; 21–34 ms |
| Phase p99 | 33–309 ms | 73–831 ms | 63–214; 86–250 ms | 457–1,496; 210–554 ms |
| Phase max | 90–563 ms | 135 ms – 7.3 s | 108–434; 188–566 ms | 1.3–8.2 s; 3.3–4.7 s |
| Verification, 500 sampled keys | 0 mismatches, 0 unresolved | 0 / 0 | 0 / 0; 0 / 0 | 0 mismatches, **18 keys with unresolved values**; 0 / 0 |
| Write amplification per worker | 3.58 x | 3.60 x | 4.23 x; 4.23 x | 3.97 x; 4.29 x |
| `stall_ms` per worker | 1–9 | 8–58 | 53–96; 20–40 | 117–295; 22–35 |
| Leader journal syncs per write | 0.94 | 1.00 | 0.83; 0.83 | 1.00; 1.00 |
| Commits per Data entry | 1.07 | 1.00 | 1.20; 1.21 | 1.00 |
| Worker CPU per write w1 / w2 / w3 / API | 8.79 / 6.28 / 5.88 / 1.57 ms | 11.68 / 7.89 / 7.07 / 1.71 ms | run 2: 6.71 / 4.76 / 4.41 / 1.26 ms | – (cgroup samples end before the window) |

- **Refusals and unresolved outcomes.** Every refusal is 429 `executor admission capacity exhausted`, refused before
  execution. The 18 unresolved updates in main's first 100/s soak are client `TimeoutError`s at 10 s during
  multi-second stalls. Reconciliation could not settle them, so that run is `correctness_verified: false`.
- **Main.** Main lacks SNAP1 (Raft snapshot export off the apply path) and RD3's leader admission. Its refusals and
  multi-second phase maxima match SNAP1's "before" soak.
- **Candidate.** The candidate has no stall windows (phase max ≤ 566 ms). Its 100/s phase p99 is still 63–250 ms, with
  driver-capped arrivals in 8 of 10 phases, against a p99 of 34 ms for 100/s in the update sweep. So the 2 KiB-row soak's
  flush and compaction still lengthen the tail. (Write amplification counts flush + compaction + journal bytes over user
  bytes.)

## G6: cold restart (SIGKILL all three workers, start on a new run, same certificates)

| | Candidate | main |
| --- | --- | --- |
| After update sweep: all ready / `data.direct.v1` active / first get through the running API | 4.7 / 5.1 / 5.8 s | 8.0 / 8.2 / 8.4 s |
| After soak 50/s | 5.3 / 5.4 / 5.6 s | 5.5 / 5.8 / 5.9 s |
| After soak 100/s, run 1 / run 2 | 6.7 / 6.8 / 7.0 s; 6.8 / 7.0 / 7.2 s | 7.8 / 8.0 / 8.2 s; 7.5 / 7.6 / 7.8 s |

The first point get succeeded on the first attempt in every case. Data per worker after the soaks was 1.1–1.3 GB.

## G7: worker memory

Largest `memory.peak` of any worker per run (always w1), in MB. It is the maximum over the 5 s cgroup samples, the
sample before a cold restart's kill and the final read (`gstats.py`). Anon and page cache (`file`) are the maxima of
`memory.stat`, in MiB.

| Workload | Candidate | main |
| --- | --- | --- |
| Serial update, run 1 / 2 | 81.4 / 75.8 (anon 37, file 30) | 71.8 / 71.6 (anon 37, file 23–25) |
| Serial get | 74.2 | 71.7 |
| Update sweep, whole run | 141.8 (anon 67, file 62; ran to 300/s) | 116.2 (anon 56, file 43; stopped at 100/s) |
| Update sweep, through 100/s | 101.4 | 105.1 |
| Get sweep, through 100 / 200/s / whole | 80.3 / 80.3 / 80.4 | 80.3 / 84.7 / 85.4 |
| Mix, through 100 / 200/s / whole | 75.9 / 77.1 / 86.0 | 81.1 / 82.6 / 88.9 |
| Soak 50/s | 862.6 (anon 93, file 589) | 1,272.7 (anon 95, file 897) |
| Soak 100/s, run 1 / 2 | 958.9 / 967.3 (anon 96, file 789) | 1,358.4 / 1,365.5 (anon 96, file 1,172) |

The "through N/s" rows read `memory.peak` at the drain end of the last phase at that rate (`peak_at.py`,
`memory-equal-load.txt`). The candidate's serial peak is 4–10 MB higher than main's, and its page cache is 5–7 MiB
higher. Under load the candidate is equal or lower at every common rate. The soak peaks are page cache, well under the
2.15 GB limit.

## Files

- `runs/<workload>-<system>[-n].json`: compact records (`cand`, `main`, `scylla`).
  - Contents: settings, binary SHA-256, image, Docker environment, per-rate summary, deferred verification, direct
    counters, client-observed refusals, cgroup I/O and CPU, worker memory, caps inspection, and the cold-restart or soak
    record.
  - `update-cr-*` are the update sweeps followed by a cold restart.
- `rates.txt`, `rates.json` (`rates.py`): per-rate CPU per ack, journal syncs per write, write bytes per write, and
  DirectBatch commits per entry with the member histogram.
- `memory-and-caps.json` (`gstats.py`), `memory-equal-load.txt` (`peak_at.py`): G7 and per-container caps.
- `storage-soak.json` (`storage_soak.py`): each worker's `nabedra-kv` counters at the end of every soak.
- `load.log`: every lane window and run start and end, with waits and load. `w4-top.log`: process samples during window 4.
- Scripts:
  - `run.sh` and `session.sh` (lane handling) ran the workloads;
  - `collect.sh` builds this directory;
  - `collect.py`, `analyze.py`, `summarize.py` and `gstats.py` are BENCH2's (`../final/`);
  - the cells come from `../summary.py`, the serial details from `../perf2e/analyze.py` and the stage means from
    `../rd3/analyze.py --stages`.
- The full outputs are in the ledger under `.nabedra/workflows/hardening/bench3/runs/`: per-request samples, worker
  logs and cgroup samples.
