# RD3: leader admission by count and memory; Data-commit scheduling hops

Controlled host (this Mac, Docker Desktop on the USB SSD `/Volumes/ZX20`, NS11 caps, driver defaults as in RD2/GC2),
2026-11-22 00:44–02:16 UTC, two sessions on the shared Docker lane (BENCH). Every run's `correctness_verified` is true
except one (see "One run with unresolved requests").

Binaries (bench artifacts, Linux x86_64):

| label | binary | SHA-256 | what it is |
| --- | --- | --- | --- |
| base | `nabedra-db-22a1934` | `262789db…f5180` | bb6dc32 (main 70c4e7d, PR #23) + RD2 |
| cand | `nabedra-db-9876be6` | `08f51726…f3dd08` | + leader direct admission (and the later reverted submit broadcast) |
| fin | `nabedra-db-123b638` | `4250f775…ece80c` | + kv journal unsynced head, inline small Raft codecs; broadcast reverted |
| `noadm` | cand or fin with `NABEDRA_BENCH_NO_DIRECT_ADMISSION=1` | | the same binary, direct requests in the ingress class's 4 handler slots (pre-RD3 admission) |

Scripts: `run.sh`, `session.sh`, `analyze.py` (from RD2's), `summary.sh`. Raw outputs stay in the hardening ledger
(`rd3/runs/`). `fixed-summary.txt` and `serial-stages.txt` are this directory's copies of the analysis.

## Fixed rates (64 clients, 5,000 rows, 8 s x 2 per rate)

Acknowledged updates per second (refused = 0 in every run):

| run | 200 | 300 | 400 | 600 | p50 at 200 (ms) | w1 CPU ms/ack at 400 | batch members at 400 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| base-1 / 2 / 3 | 181.4 / 181.7 / 176.1 | 176.2 / 174.6 / 174.1 | 168.4 / 178.9 / 175.9 | 168.6 / 171.8 / 173.5 | 245 / 265 / 257 | 5.36 / 5.06 / 5.15 | 2 (1.50 commits/entry) |
| cand-noadm-1 / 2 | 182.1 / 142.4 | 169.6 / 162.1 | 168.6 / 158.7 | 169.6 / 171.2 | 263 / 399 | 5.30 / 5.63 | 2 |
| cand-1 / 2 / 3 / 4 | 199.9 / 199.9 / 199.8 / 199.5 | 295.8 / 291.7 / 289.4 / 287.0 | 288.7 / 282.4 / 288.1 / 285.9 | 285.1 / 275.4 / 269.7 / 281.5 | 17.9 / 18.4 / 16.4 / 17.2 | 2.56 / 2.69 / 2.57 / 2.56 | up to 16 (4.5 commits/entry) |
| fin-noadm-1 / 2 | 196.0 / 195.9 | 227.0 / 227.8 | 215.7 / 216.7 | 215.6 / 219.0 | 17.0 / 19.9 | 4.12 / 4.09 | 2 |
| fin-1 / 2 / 3 | 200.0 / 199.9 / 199.9 | 284.2 / 284.2 / 292.4 | 294.2 / 263.3 / 282.8 | 271.9 / 266.1 / 283.3 | 11.9 / 14.9 / 12.3 | 2.38 / 2.82 / 2.44 | up to 32 (3.6 commits/entry) |

- Leader admission alone (cand vs cand-noadm, same binary): ~170 → ~285–295 acked/s; batches from 2 members to 8–16.
- The journal and codec changes alone (fin-noadm vs cand-noadm, both with pre-RD3 admission): ~160–170 → ~216–228.
- Both (fin): 200/s at p50 12 ms (base ~250 ms), ~285–295 acked/s at 300–600 offered.
- The plateau near 290/s: the batcher keeps one proposal in flight per range, and at 600 offered the API spends
  ~28 ms reading request bodies (`http.dispatch.body`); `leader.attempt.propose` is ~15 ms (the previous batch's commit
  plus its own). Not changed here.

## Serial (0.2/s, 10 requests per run), means in ms

| run | client p50 | API dispatch | `client_write` | `append_encode` | AppendEntries sent | follower handle | first ack | commit | apply start |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| base-1..4 | 20.9 / 22.8 / 23.7 / 21.8 | 18.4–20.0 | 14.7 / 15.1 / 15.7 / 14.7 | 0.61–0.84 | 1.85–2.07 | 8.2–8.9 | – | – | 13.6–14.8 |
| cand-1..4 | 20.2 / 19.0 / 22.6 / 18.8 | 15.9–19.0 | 14.6 / 12.9 / 15.3 / 12.8 | 0.47–0.80 | 1.41–1.89 | 7.1–9.0 | 10.4–12.9 | 10.4–13.0 | 11.9–14.4 |
| cand-nobc-1, 2 (broadcast off) | 18.0 / 18.2 | 16.9–17.6 | 14.7 / 13.1 | 0.52–0.72 | 1.50–1.56 | 7.6–8.0 | 10.9–11.3 | 11.0–11.4 | 12.3–12.6 |
| fin-1, 2 | 15.3 / 21.2 | 14.2 / 16.9 | **10.8 / 13.1** | **0.02** | **0.35 / 0.43** | 7.4 / 9.0 | 8.7 / 10.6 | 8.8 / 10.7 | 10.0 / 12.2 |

- `cand-nobc1` overlapped a local `cargo check` for about 40 s; treat it with care. `nobc2` agrees with it.
- Quorum → commit is 0.05 ms; commit → apply start ~1.3 ms on the host and ~0.4 ms on the runner (see "Not
  changed").
- The submit broadcast (`broadcast_submitted_on_append`) gave nothing measurable and was reverted.

## Runner (`perf` job, same workflow, different runs: `9876be6` run 44966525243, `123b638` run 44971632185)

| | 9876be6 | 123b638 |
| --- | ---: | ---: |
| serial p50 | 3.7 ms | 3.2 ms |
| serial `client_write` | 1.98 ms | 1.30 ms |
| `append_encode` | 0.145 ms | 0.011 ms |
| AppendEntries sent after proposal | 0.248 ms | 0.175 ms |
| follower `append_handle` | 0.49 ms | 0.36 ms |
| 50/s p50 / p99 | 3.2 / 6.4 ms | 2.3 / 3.9 ms |

## One run with unresolved requests

`fixed-fin-noadm1`, 200/s rep 1: 4 requests failed with `ConnectionResetError` on the driver → API HTTPS connection in
one burst, while the driver was opening connections (32 `client_capacity` refusals in the same second). Reconciliation
answers `not_applied`; the final snapshot has 0 mismatches and none of the 4 values, so nothing was applied. It did not
reproduce in the repeat (`fixed-fin-noadm2`) or in any of the other 14 fixed runs. Its likely place is the API's
keep-alive handling (PR #23's oldest-idle eviction racing a request sent on that connection); the API code is the same
in every binary here.
