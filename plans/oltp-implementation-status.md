Implementation tracking for [the OLTP design](oltp-engine.md). Work remains on the design thread's existing branch, `nabedra/oltp-engine-plan`.

| Track | Deliverable | Owner | Status |
| --- | --- | --- | --- |
| P0 | Dependency feasibility and transaction/recovery argument, including distributed fences/timestamps | Storage, integration | Native spike compiled; local and conditional distributed arguments written; distributed assertions pending |
| P1 | Mutually exclusive engine builds, optional native dependencies, matching ORM count decoder | Packaging agent | Four-selection matrix, dependency graphs, identities, invalid guards and Python assertions passed; Linux/MSRV CI pending |
| P2, P3 primary path | Indexed durable local engine, exact operation semantics, bounded query execution | Storage agent | Local assertions executed; shared pool/group extraction pending |
| P3 secondary path | Secondary/covering indexes and online index publication | Storage agent | Pending |
| P6 local | Standalone authenticated API, bounded admission, receipts and cold-start behavior | Runtime agent | Library and quiet native process/TLS assertions passed; unreadable decision-proof certainty regression remains to fix |
| P4 foundation | Durable Raft log, authenticated transport, deterministic state-machine application and streamed snapshots | Packaging, storage agents | Four single-group replication assertions passed, including three mTLS peers; typed distributed transaction integration pending |
| P4–P5 integration | Replicated data shards, timestamps and atomic distributed decisions | Integration | Pending |
| P6 distributed | Replicated RAM admission, owner takeover and shard-locality scheduling | Runtime agent | Pending |
| P7 | Safe shard splits, pool/worker movement and mounted-volume identity | Unassigned | Pending |
| P8 | Fixed cloud deployment, consistent backup/restore and offline migration | Unassigned | Pending |
| P9–P11 | Capacity planning, provider-backed storage growth and safe autoscaling | Unassigned | Pending |
| P12 | Contract assertions, fault tests, benchmarks and provider acceptance | Integration | Pending |

The local engine is not the distributed production release. Acceptance identifiers and executed evidence are recorded in [the evidence ledger](../docs/testing/oltp-evidence.md); unexecuted assertions remain pending. The frozen checkpoint passed all four engine selections, 178 Python tests and 16 harness tests. The quiet native process capture records its exact binary and source hashes. Default Clippy retains 23 inherited errors; OLTP Clippy passes with warnings denied. These results do not establish the complete distributed design.

The [serialization argument](../docs/architecture/oltp-serialization.md) lists the assumptions that the distributed implementation and fault histories must establish. It does not infer correctness from the local storage or consensus-library tests.

The internal SSD initially had approximately 330 MiB free. Free space subsequently increased to more than 20 GiB, allowing native compilation. Builds reuse the primary checkout's Rust target cache. This implementation did not remove user data or caches.
