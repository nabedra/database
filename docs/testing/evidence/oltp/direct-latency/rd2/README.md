# RD2 evidence: Timestamp hop, Data commit breakdown, batching through the API

Binary `nabedra-db-22a1934` (RD2 on `bb6dc32`, `[ci bench]` run 44933727753, SHA-256 `262789db…f5180`).
The base is the same binary with `NABEDRA_BENCH_NO_GRANT_WARM=1` (bb6dc32 behaviour plus timers), alternated in
one session. Controlled host: this Mac, Docker Desktop on the external USB SSD `/Volumes/ZX20`, NS11 caps,
2026-11-21 18:32–18:47Z, Docker lane held with `RD2 BENCH`. Each step waited for a 1-minute load below 4 (see
`load.txt`; the 15-minute load was still 20–45 from other threads' builds).

Files:

- `host-summary.txt`: per rate, acked/s, refusals, p50/p99, CPU ms per ack, DirectBatch counters.
- `host-serial-stages.txt`: stage means per serial run (warm on: `on1`, `on2`; off: `off1`, `off2`).
- `runner-22a1934.txt`: `[ci perf]` runner, same binary and runner, serial warm on vs off, 50/s and 150/s.
- `analyze.py`, `run.sh`, `session.sh`: the scripts. Raw runs stay in the lead's ledger (`LEDGER/rd2/runs`).

New stages: `sess.c.*` / `sess.s.*` per traffic class, `grant.role.{hit,miss}`, `grant.warm.refresh`,
`raft.data.propose_to_{append,send,flush,ack,apply}` (serial-only), `pool.data_apply.{order,prepare,execute,commit}`.
