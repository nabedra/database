Implementation tracking for [the OLTP design](oltp-engine.md). Work remains on the design thread's existing branch, `nabedra/oltp-engine-plan`.

| Track | Deliverable | Owner | Status |
| --- | --- | --- | --- |
| P0 | Dependency feasibility and transaction/recovery argument, including distributed fences/timestamps | Storage, integration | Native spike compiled; local and conditional distributed arguments written; distributed assertions pending |
| P1 | Mutually exclusive engine builds, optional native dependencies, matching ORM count decoder | Packaging agent | Mac/Linux Rust 1.94.1 feature checks and Linux four-selection tests passed; 179 Python tests passed on both platforms after portability fix; GitHub CI pending |
| P2, P3 primary path | Indexed durable local engine, exact operation semantics, bounded query execution | Storage agent | Shared pool/group/snapshot and native thread assertions passed; bounded scan scratch and MVCC/runtime integration in progress |
| P3 secondary path | Secondary/covering indexes and online index publication | Storage agent | Pending |
| P6 local | Standalone authenticated API, bounded admission, receipts and cold-start behavior | Runtime agent | Library and quiet native process/TLS assertions passed; unreadable decision-proof regression fixed in `607d5fe` and follow-up library run passed |
| P4 foundation | Durable Raft log, authenticated transport, deterministic state-machine application and streamed snapshots | Packaging, storage agents | Four single-group replication assertions and multiplex transport passed; typed entry/log adapters in progress |
| P4–P5 integration | Replicated data shards, timestamps and atomic distributed decisions | All implementation agents | Actual Control/TSO/Status state machines and leader services in progress; MVCC Prepare/Decide/Resolve remains pending |
| P6 distributed | Replicated RAM admission, owner takeover and shard-locality scheduling | Runtime agent | Five worker/API process assertions passed with fixture authority, including exact 8 MiB copy/join and predicate depth; real Control/transaction integration and terminal retirement pending |
| P7 | Safe shard splits, pool/worker movement and mounted-volume identity | Unassigned | Pending |
| P8 | Fixed cloud deployment, consistent backup/restore and offline migration | Unassigned | Pending |
| P9–P11 | Capacity planning, provider-backed storage growth and safe autoscaling | Unassigned | Pending |
| P12 | Contract assertions, fault tests, benchmarks and provider acceptance | Integration | Pending |

The local engine is experimental. Acceptance identifiers and executed evidence are recorded in [the evidence ledger](../docs/testing/oltp-evidence.md); unexecuted assertions remain pending. The shared foundation is committed as `cb38250`. Its complete Linux pipeline passed all four engine selections, 179 Python tests and 16 harness tests. The quiet native process captures record exact binary and source hashes. Default Clippy retains 23 inherited errors at its recorded audit; OLTP Clippy passed on the foundation. Newer authority and MVCC changes require separate checks. These results do not establish the complete distributed design.

The [serialization argument](../docs/architecture/oltp-serialization.md) lists the assumptions that the distributed implementation and fault histories must establish. It does not infer correctness from the local storage or consensus-library tests.

The measured source is committed as `f958c737d8c1010db7b5dfeda9977f4bce3e2ed5`. All four selections passed native macOS all-target compilation under Rust 1.94.1 after the inherited OLAP test import correction in `251729c`. The decision-proof correction is committed separately as `607d5fe`. Linux checkpoint `251729c` passed the four Rust selections, native process and harness assertions with two CPUs and 6 GiB RAM. Its Python failures led to the separately verified portability fix in `dbee65d`, after which 179 tests passed on Mac and Linux. These checkpoints are separate from continuing distributed implementation and do not establish GitHub CI or provider acceptance.

The subsequent [Linux foundation evidence](../docs/testing/evidence/oltp/linux-cb38250/README.md) verifies immutable `cb38250` under Rust 1.94.1 with the same two-CPU, 6 GiB container limits. Default OLAP passed 759 tests, OLAP without S3 passed 757, and each OLTP selection passed 143. Native server/ORM, Python and harness stages passed in that same pipeline. The live-S3 assertion remains unexecuted. A separate check of this commit passed minimum-toolchain formatting on macOS.

The internal SSD initially had approximately 330 MiB free. Free space subsequently increased to more than 20 GiB, allowing native compilation. Builds reuse the primary checkout's Rust target cache. This implementation did not remove user data or caches.
