# OLAP streaming simulation

The `olap-stream-sim` check runs `server/src/node/stream/sim_tests.rs`. It covers
OLAP-STR-G01 to G06 in simulation (S) with history-style checks (H). The nightly
variant `olap-stream-sim-nightly` runs the same property with more cases.

```sh
cd server
NABEDRA_CASES=200 cargo test --locked --lib node::stream::sim_tests:: -- --nocapture
# Replay one failing case from its printed seed:
NABEDRA_SEED=<seed> NABEDRA_CASES=1 cargo test --lib node::stream::sim_tests::st01_st06
# One summary line per case:
NABEDRA_SIM_TRACE=1 NABEDRA_CASES=20 cargo test --lib node::stream::sim_tests::st01_st06 -- --nocapture
```

## What each case does

A `nabedra-check` seed generates a table of up to 48 sparse keys, inserted out
of order in several commits. The partition size is 200 B to 4 KiB, so the
pinned manifest holds 1 to 48 files. The seed also picks a stream body:
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
| cold start | Every worker stops, and a new run fences the catalog. Old handles and old-run opens must be abandoned, and a new open must pin the latest data. | G05 |

## Checks

- **Oracle (G01).** The harness keeps its own row model. The oracle sorts the
  pinned rows by key and applies the filter and limit with its own evaluator,
  not the engine's. The client deduplicates delivered frames by sequence. The
  sequences must be contiguous from 0 and end in one EOF. The rows must equal
  the oracle in order, and every batch except the last must hold exactly
  `batch_rows` rows. `drop_last` removes a short final batch.
- **Replay (G02, G03).** Every repeated delivery of a sequence, including one
  after a lost reply or a kill, must be byte-identical to the first.
- **Rescan counter (G06).** The store wrapper logs every partition GET made
  during a stream call. The counter adds up rows scanned. Rows scanned from a
  file before the checkpoint's first file count as rows behind the
  checkpoint, and that count must be 0. Recovery may re-read the checkpoint's
  own file to rebuild the current batch.

## Seams: what is real and what is simulated

| Part | In the simulation |
| --- | --- |
| Coordinator (`Local`, `open_stream`, `reopen`, `demand`, `scan_one`, prefetch, the registry) | Real code, called in process |
| Engine scan reader, Parquet decoding, batch encoder, catalog fences, table commits | Real code |
| Object store | `SimObjectStore`, with seeded 503 faults on GET and a counting wrapper |
| Replica holder | The harness. After each completed call it copies the coordinator's record through its serialized form. |
| Coordinator kill and recovery | Drop the `Local` registry, so its caches, prefetched output and reader go with it. Then take over the catalog fence at the next scheduler term and install the replicated record into a fresh registry. |
| Cold start | Drop the registry and fence a new run with the cold-start transition |
| Distributed recovery protocol (peer paging, `RecoveryView`, `mark_recovered`, worker transport) | Not simulated. `node/stream/distributed.rs` drives it only on real workers. The process evidence in `evidence/olap-streaming/` covers it. |
| HTTP API and ORM clients | Not simulated |

The kill cut "before replication" models a replica that holds the previous
checkpoint. The distributed `commit` installs the record locally, transfers it
to every ready peer, and only then replies. This cut matches a crash after the
local install and before the transfers.

## Planted bugs

Each planted bug is a test-only variant of the coordinator behaviour that the
harness models. Each test runs the property with the bug switched on and
asserts that it fails for the expected reason.

| Test | Bug | Caught by |
| --- | --- | --- |
| `st04_st06_planted_rescan_after_reconnect_is_caught` | A reconnect rebuilds from the first cursor and silently replays the acknowledged batches | G06 counter. The content is still correct. |
| `st04_planted_sequence_gap_after_kill_is_caught` | Recovery restores the checkpoint cursors with the previous sequence number | G03 replay check or G01 gap check, depending on the kill cut |
| `st04_planted_replay_with_new_handle_is_caught` | An open retry after an identity loss creates a new handle | G02 |

`NABEDRA_PLANTED_CASES` (default 120) bounds each planted run. Each planted
bug has so far been caught within the first five cases.
