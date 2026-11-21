# PERF2c: idle Control load and the client ↔ API leg

Measured on 2026-11-21. Base is `7efcd23`: `697bbb7` plus counters only. The candidate is `1feeb07`, which adds keep-alive and the Control idle gate. Both binaries come from the `[ci bench]` job:

- base: `nabedra-db-7efcd23`, sha256 `f97e0f20…`, run 44853988039;
- candidate: `nabedra-db-1feeb07`, sha256 `acdcc665…`, run 44857409138.

Runner figures come from the `[ci perf]` job of the same two runs.

## Verdict

- **Client ↔ API leg.** The OLTP API answered every request with `Connection: close`. The shared listener disabled HTTP/1.1 keep-alive unless the `olap` feature was built, so every ORM or driver call paid a new TCP connect and a full TLS handshake, even though both ORMs pool connections. With keep-alive after 2xx responses:
  - **Controlled host, gets.** Client p50 drops from 10.5–12.9 ms to 5.7–6.8 ms at 1–50/s. That reaches ScyllaDB's 6.6 ms at 2, 20 and 50/s. The client → `dispatch()` gap falls from 4.3–6.9 ms to 1.1–1.5 ms.
  - **Controlled host, sweep length.** The get sweep held 100/s (p50 6.5 ms) where the base build stopped at 50/s.
  - **Runner, updates at 50/s.** p50 6.7 → 4.6 ms and p99 13.8 → 9.3 ms. API CPU drops 0.088 → 0.027 and driver CPU 0.089 → 0.024.
- **Serial pace (one call per 5 s) is unchanged.** The ORMs, and the driver that mirrors them, reuse a connection only while it has been idle under 2 s, because the server closes idle connections at its 5 s header-read timeout. A serial call therefore still opens a new connection. That costs about 7–8 ms on the controlled host (3.3 ms server-side TLS accept), and about 3.5 ms on the runner. This cost comes from the protocol and the idle bound. ScyllaDB's driver keeps its connections open and has no TLS.
- **Idle Control ReadIndex.** It goes from 28.1 to 14.3 rounds/s on the Control leader (runner) and from 27.2 to 13.7/s (controlled host). Meta heartbeats drop from 63.7 to 36.5/s and native-executor hops from 69 to 42/s.
  - **Idle CPU (controlled host).** In a near-idle run (0.2 updates/s, one 60 s phase), `w1` CPU drops from 0.108 to 0.071 and each follower's from 0.060 to 0.044.
  - **Throttling.** It was already 0 in that run for both builds.
- **PERF1's 0.45 idle CPU and CFS throttling were not idle load.** The benchmark's queued snapshot check between phases spends about half the wall time of a serial or sweep run and runs on `w1`: `w1` stays at 0.62–0.69 CPU through every get rate, in both builds. The p99 spikes PERF1 tied to throttling belong to that check and to request work, not to the Control barrier rate.

## Callers of the idle Control ReadIndex (runner, serial run, per second)

| Call site (`control.barrier.site.*`) | Method | w1 | w2 | w3 | after |
| --- | --- | ---: | ---: | ---: | --- |
| `control/service.rs:1700` | `authority` (`AuthorityReader::current`) | 5.14 | 4.92 | 4.98 | w1 1.66 (`:1705`), rest gone |
| `control/service.rs:962` | `inspect` | 8.94 | 0.98 | 1.00 | w1 5.46 (`:967`) |
| `service/worker_drain.rs:69` | `active_worker_drain` | 1.00 | | | 0.98 |
| `control/service.rs:1346`, `service/group_damage.rs:142`, `:953` | `service_role_inner`, `group_damage_page`, `group` | 1.2 | | | 1.0 |

`w1` serves the followers' barriers (`control.barrier.served`: 11.9 → 5.0/s) on top of its own (`control.barrier.local`: 16.2 → 9.3/s).

The main caller was the registered queue-membership loop. Every worker ran `current` every 250 ms, and the queue owner also ran `inspect`. Now a pass that found nothing to do is repeated only when one of three things happens:

- the worker's Control replica has applied a new entry (openraft's in-memory `last_applied`);
- the local queue authority has changed;
- 2 s have passed.

Every pass that runs still reads through the ReadIndex barrier.

What remains:
- `supervise_membership` probes peers once per second per worker for D05 failure detection; its rate is deliberate.
- About 4/s of `inspect` on `w1` that come with retained requests. These are probably `dispatch_retained` while the benchmark's queued snapshot checks are retained; not confirmed.

## Stages, controlled host (means per operation, ms)

Get sweep (`get-*`, 3 × 5 s per rate):

| Rate | | client p50 | client − dispatch | API TLS accept per op | `http.dispatch.total` | `leader.get.total` |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1/s | base | 11.6 | 6.88 | 4.56 | 4.74 | 1.98 |
| | cand | 6.8 | 1.53 | 1.34 | 5.25 | 2.45 |
| 20/s | base | 10.5 | 6.05 | 3.67 | 4.49 | 1.70 |
| | cand | 5.7 | 1.08 | 0.32 | 4.60 | 2.19 |
| 50/s | base | 12.9 | 4.27 | 5.34 | 8.58 | 2.22 |
| | cand | 6.2 | 1.35 | 0.10 | 4.90 | 2.02 |
| 100/s | base | (sweep stopped at 50/s, p99 105 ms) | | | | |
| | cand | 6.5 | 0.36 | 0.10 | 6.12 | 2.00 |

Serial update (`serial-*`, 10 × 5 s at 0.2/s) and near-idle (`idle-*`, 60 s at 0.2/s):

| Run | client p50 | client − dispatch | `leader.mutation.total` | timestamp | propose | `raft.data.append` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| serial base | 36.0 | 7.2 | 24.3 | 9.6 | 12.7 | 2.4 |
| serial cand | 43.5 | 7.8 | 30.4 | 11.2 | 17.0 | 3.7 |
| idle base | 35.7 | 8.1 | 24.0 | 7.6 | 14.1 | 2.3 |
| idle cand | 47.8 | – | 50.7 | 7.3 | 41.3 | 11.7 |

The candidate's slower serial runs are in the Data commit's fsync (`raft.data.append` 11.7 ms in idle-cand). Another agent's `rustc` build ran during these runs (host load 7–11), and this change does not touch that path. On the quiet runner, serial is 9.2 ms (base) against 9.3 ms (candidate).

Container CPU (median of 5 s cgroup intervals inside the measured phase):

| Run | w1 | w2 | w3 | api | w1 throttled periods |
| --- | ---: | ---: | ---: | ---: | --- |
| idle base | 0.108 | 0.060 | 0.061 | 0.016 | 0/540 |
| idle cand | 0.071 | 0.044 | 0.045 | 0.014 | 0/521 |
| get 50/s base | 0.689 | | | 0.070 (32/448 throttled) | 30/560 |
| get 50/s cand | 0.622 | | | 0.042 (10/388 throttled) | 16/477 |

Driver at 50 gets/s: base opened 254 connections per phase and used 0.251 CPU; candidate opened 9 and used 0.091 CPU. Its cgroup (0.65 CPU) was not throttled.

## What was checked on the client ↔ API leg

- **HTTP read and response write.** `http.dispatch.body` is 0.01–0.05 ms and `spawn_wait` is 0.02–0.1 ms. The leg was the per-request connection, not hyper.
- **TLS.** `http.accept.handshake` (TCP accept to the end of the TLS handshake) takes 3.3–3.8 ms under the API's 0.35 CPU, against 1.9–2.1 ms on the runner. A reused connection pays none of it.
- **`TCP_NODELAY`.** Accepted sockets did not set it. They do now. It did not explain the leg: a response is one write.
- **One TLS record per response.** This is from reading the code; no packet capture was taken. The listener's `Connection` wrapper does not implement vectored writes, so hyper flattens the head and the body into one buffer and makes one `poll_write`. tokio-rustls seals that write as one record for responses under 16 KiB.
- **Driver CPU.** At most 0.25 of its 0.65 CPU at 50/s on the base build, never throttled. At serial pace it is negligible.
- **API container throttling.** It is visible on the base build at 50/s (32 of 448 periods) and drops with keep-alive (10 of 388).

## Files

- `runs/<run>/results.json`: compact driver records, without the raw before/after stats.
- `runs/<run>/cgroup-samples.jsonl`: 5 s cgroup samples of w1–w3 and the API.
- `runner-*` records come from the `[ci perf]` artifacts.
- `analyze.py <run>...`: per-rate p50/p99, connection reuse, handshake, Control ReadIndex/s, driver CPU.
- `cgroup_intervals.py <slack s> <run>...`: per-interval container CPU and throttling inside the measured phases.
- `../stages.py`: per-stage tables.
- `load.log`: start and end times and host load of every controlled-host run. The lane was held from 02:56Z to 03:19Z, starting at a 1-minute load of 2.75.
