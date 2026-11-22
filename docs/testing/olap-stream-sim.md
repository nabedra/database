# OLAP streaming simulation

The `olap-stream-sim` check runs `server/src/node/stream/sim_tests.rs`. It covers
OLAP-STR-G01 to G06 in simulation (S) with history-style checks (H). The nightly
variant `olap-stream-sim-nightly` runs the same property with 2,000 cases
in the scheduled T2 run and at T3. T1 runs 200 cases and the three
sensitivity tests. The command timeouts include compilation and are
30 minutes for T1 and 120 minutes for the longer run.

```sh
cd server
NABEDRA_CASES=200 cargo test -j 2 --locked --lib node::stream::sim_tests:: -- --nocapture --test-threads=1
# Replay one failing case from its printed seed:
NABEDRA_SEED=<seed> NABEDRA_CASES=1 cargo test -j 2 --locked --lib node::stream::sim_tests::st01_st06
# One summary line per case:
NABEDRA_SIM_TRACE=1 NABEDRA_CASES=20 cargo test -j 2 --locked --lib node::stream::sim_tests::st01_st06 -- --nocapture
```

## What each case does

A `nabedra-check` seed generates a table of up to 48 sparse keys, inserted out
of order in several commits. The partition size is 200 B to 4 KiB, so the
pinned manifest holds 0 to 48 files. The seed also picks a stream body with
`batch_rows` from 1 to 7, a prefetch depth from 0 to 2, `drop_last`, and an
optional key-range filter and limit. The client then runs up to 160 nemesis
steps and reads to EOF:

| Step | What happens | Guarantee |
| --- | --- | --- |
| advance | Demand the next sequence and receive it | G01, G03 |
| lost reply | The coordinator commits the demand, and the client never sees the reply | G03 |
| probe | Demand the current sequence again, then stale, future and post-EOF sequences, which must conflict | G03 |
| reopen | Open again with the same UUID and body, which must return the same handle. A conflicting body with that UUID must conflict. Some cases race two opens. | G02 |
| kill | Kill the coordinator between demand and ack, either after or before the checkpoint replicated, or while idle. Recover on a new owner under a takeover fence. | G04 |
| commit | Insert, delete or upsert rows while the stream is open | G01 (ConsistentRead) |
| GET faults | Seeded 503s from `SimObjectStore` during one demand | G03, G06 |
| cold start | The harness leaves an open outstanding, drops the registry, and starts a new run on the same catalog. Old handles and old-run opens must be abandoned, and a new open must pin the latest data. | G05 |

## Checks

- **Oracle (G01).** The harness records successful writes in its own row map.
  The oracle iterates the pinned map in primary-key order, evaluates the
  filter and limit, and splits the expected rows into batches. The client
  deduplicates delivered frames by sequence. The
  sequences must be contiguous from 0 and end in one EOF. The rows must equal
  the oracle in order, and every batch except the last must hold exactly
  `batch_rows` rows. `drop_last` removes a short final batch.
- **Replay (G02, G03).** Every repeated delivery of a sequence, including one
  after a lost reply or a kill, must be byte-identical to the first.
- **Rescan counter (G06).** The store wrapper logs successful partition GETs
  from the stream engine, including background prefetch between calls.
  Mutation reads use a separate wrapper over the same store. The counter
  charges the manifest's row count for each file read. A successful read of
  a file before the retained checkpoint's first file fails the check.
  Recovery may re-read the checkpoint's own file to rebuild the current
  batch. File-level accounting cannot detect redundant work within that file.

## Seams: what is real and what is simulated

| Part | In the simulation |
| --- | --- |
| Coordinator (`Local`, `open_stream`, `reopen`, `demand`, `scan_one`, prefetch, the registry) | Real code, called in process |
| Engine scan reader, Parquet decoding, batch encoder, catalog fences, table commits | Real code |
| Object store | `SimObjectStore`, with seeded 503 faults on GET and a counting wrapper |
| Replica holder | The harness snapshots the serialized entry at the selected failure cut. |
| Coordinator kill and recovery | Drop `Local` to stop its registry and clear cached output and readers. Advance the catalog fence within the same run and restore the holder's entry into a fresh registry via `Registry::install_fenced`, the entry point used by distributed recovery. |
| Cold start | Drop the registry and fence a new run with the cold-start transition |
| Distributed recovery protocol | Process tests exercise `node/stream/distributed.rs`. See [OLAP streaming process evidence](evidence/olap-streaming/README.md). |
| HTTP API and ORM clients | Outside this workload |

Distributed commit installs the checkpoint locally, transfers it to the
ready peers, rechecks authority, then replies. At the pre-replication cut,
a test driver lets the real local install succeed and interrupts the shared
demand path before it can reply. The holder supplies the previous entry.
At the post-replication cut, the holder supplies the new entry and the client
loses the reply. The idle cut supplies the last retained entry.

The simulation starts recovery with one complete surviving entry and a blank
registry. It can catch failures in fenced installation and in rebuilding and
replaying the pinned batch. Peer discovery, paging, merging divergent copies,
repairing replicas, recovery-view checks, signed transport and process
termination still require separate evidence. The in-process run shares the
worker pool and object store across owners. ST01 to ST06 identify the clauses
exercised here. The workload covers row frames, replay and file-level scan
work. The streaming plan also requires process, ORM and performance checks.

## Planted bugs

Each planted bug is a test-only variant of the coordinator behaviour that the
harness models. Each test runs the property with the bug switched on and
asserts that it fails for the expected reason.

| Test | Bug | Caught by |
| --- | --- | --- |
| `st04_st06_planted_rescan_after_reconnect_is_caught` | A reconnect rebuilds from the first cursor and silently replays the acknowledged batches | G06 counter. The content is still correct. |
| `st04_planted_sequence_gap_after_kill_is_caught` | Recovery restores the checkpoint cursors with the previous sequence number | G03 replay check |
| `st04_planted_replay_with_new_handle_is_caught` | An open retry after an identity loss creates a new handle | G02 |

`NABEDRA_PLANTED_CASES` (default 120) bounds each planted run. Each planted
bug has so far been caught within the first five cases.
