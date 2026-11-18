# Docker acceptance evidence for the OLAP sink

The SDK and sink evidence came from the standalone Events broker, the Python
client, and the existing OLAP HTTP API and worker path. The OLAP storage
adapter talked to the repository's SQLite conditional object store over
loopback. The runtime container used `--network none`.

| Run | CPU quota | Memory limit | Result |
| --- | ---: | ---: | --- |
| OLAP build, Rust 1.94.1, locked, `--no-default-features` | 1 | 2 GiB | Passed in 7m 53s. Peak 1,885,589,504 bytes, zero OOM events. |
| Events SDK and sink configuration, Python 3.11.2 | 0.25 | 192 MiB | Five passed in 3.311s. Peak 23,605,248 bytes, zero OOM events. |
| EV04 initial run, Python 3.11.2 and real Linux binaries | 1 | 576 MiB | Nine scoped assertions passed in 199.908s. Peak 147,836,928 bytes, zero OOM events. |
| EV04 final run after the rejoining-learner fix | 1 | 576 MiB | The same nine assertions passed in 207.894s. Peak 138,665,984 bytes, zero OOM events. |
| OLAP queue overflow supplement, seed 20261115 | 1 | 576 MiB | The request-count and replica-byte runs passed. Peak 95,416,320 bytes, zero OOM events. |

The OLAP build used one Cargo job with incremental compilation and dev
debug info disabled. The OLAP SHA-256 was
`170a1491ec6f1482c0acc7eca6893013a520652d82c0a58ba7b6a5f5ba855752`.
The initial Events SHA-256 was
`9a9b5ce80d04b2f76d7d811e462ca539181c2bc43d86fb4dd1bc5267277b98b1`,
and the final Events SHA-256 was
`b684a76a68b2361a1bc120dd7e3acdf7abbc4770076a43678b1f7fcf540a7ba9`.
Both used RocksDB 11.8.1 shared libraries. The Docker VM reported
4,303,944 kB available before the initial run and 6,052,388 kB before the
final run. The VM had eight CPUs and 8,220,106,752 bytes of RAM.
Other broker and database validation jobs were active, and the runner
reported CPU quota throttling. The elapsed times are fixture observations.

## Executed assertions

- Two sink lanes consumed separate partitions while an independent subscription
  read both. A proxy withheld one real OLAP submission reply until the receipt
  showed a successful commit and the store contained one durable decision.
  The second partition advanced while the reply was withheld.
- The sink was killed while its checkpoint PUT was held before reaching Events.
  The affected checkpoint stayed at zero. On restart, redelivery advanced the
  checkpoint and the four row identities and contents matched the oracle,
  including the exact JSON number `1.2345678901234567890123456789`.
- Q04 and R01 API restart and worker-loss assertions. The sink's accepted
  mutation was held at an actual worker-job barrier. The restarted API returned
  the original, still-pending run and request UUID. Killing the scheduler
  worker left one surviving ready worker. The sink observed the same
  successful receipt, the store contained one decision, and the checkpoint
  advanced after the commit.
- R04 full-stop assertions. All workers stopped with a second accepted mutation
  held at its job barrier. The cold-start API reported that receipt abandoned
  and retained earlier committed results. The sink submitted a fresh identity,
  with one durable decision, and the checkpoint advanced with all exact rows.
- The fixture held the sink's sole in-flight permit
  while a second partition delivered events and released its lease. Neither
  checkpoint advanced until the first mutation settled. Both then committed.
- T06 stale-destination and conflict assertions. A same-name table replacement,
  an incompatible schema, and a preexisting row with the same event identity
  and different data each paused the sink. The destination rows and relevant
  source checkpoint matched the expected state.

The existing scheduler fixture ran with `--suite overflow` against the
same OLAP binary.

- Q02 request-count limit. The fixture held eight accepted requests and the
  ninth returned 429 `overloaded`. All accepted requests retained their
  pending receipts and completed with their expected rows when gates opened.
- Q02 replica-byte limit. With a 40,000-byte budget and a separate limit of
  100 requests, it held three accepted requests and the next returned 429.
  All original receipts and terminal results matched their oracles.
- U02 subset. During both overflow runs, the API returned 400
  `validation_error` for incomplete JSON, a duplicate request field, and a
  wrong table ID type. It returned 401 `unauthorized` for the invalid token.
  The held valid requests still completed. These assertions cover four
  input cases in the scheduler contract.

The public-API loss test does not exercise loss of the object-store catalog
PUT response. The cold-start worker-job gate tests abandonment before
publication. Q05, full Q06, T04, T05, and U01 require their own fixtures.
The sink permit assertion is independent of the OLAP queue admission limits.

The small OLAP smoke exposed a direct-endpoint configuration problem.
Sending the gateway-only database selector to the dedicated API returned
`storage context not authorized`. With the selector omitted, the direct
API accepted a queued mutation, returned its exact receipt, kept one
durable decision and returned the original row. The sink now accepts
both dedicated endpoint and gateway configurations.

The full ORM discovery ran 182 tests in Docker and reported eight failures
in existing CLI supervision and serve lifecycle cases. A follow-up with
`--init` ran the same 24 unchanged supervisor tests on main and on this
worktree. Main reported eight failures. This worktree reported six failures
and two errors. The Events SDK run and EV04 results above are separate
from that broader baseline result.

To repeat the overflow supplement with the same Docker caps and mounts, run
`python scripts/acceptance_scheduler_load.py --binary /olap-bin/nabedra-db
--output /evidence/queue-overflow.json --suite overflow` in place of the sink
fixture. The mounted worktree's Git metadata was outside the container, so
the fixture's optional commit field was empty. Its database source digest
was `428f9c6eb43d06bececaa7b82525c52a134f73d6b16a88b7cb67242a6bf76069`.

See [the Docker commands](README.md) and
[the process fixture](acceptance_sink.py) to reproduce the checks.

## Source-linked broker verification

The broker built against vendored RocksDB passed the same nine EV04
assertions in 249.977s with the same OLAP binary and the same
1 CPU and 576 MiB Docker caps. The source-linked Events SHA-256 was
`f1242473f18608298f4e8f0dca4f1b8787592e81b347519cb7f9bcfeb68ae1b7`.
The runtime used the system libraries in the Docker image. The Docker VM
reported 6,377,188 kB available before startup. The cgroup peaked at
200,884,224 bytes and recorded zero OOM events.
