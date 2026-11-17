Implementation continues on `nabedra/oltp-engine-plan` in the existing design worktree. The full module is **not finished**. This is the finite implementation checklist, not a claim of acceptance. Historical build and component results remain in [the evidence ledger](../docs/testing/oltp-evidence.md).

The lead only orchestrates, maintains this table, coordinates interfaces and assesses results. All implementation, integration and test coding belongs to the agents. Runtime owns the central acceptance runner and shared process/ORM helpers; scenario assertions belong to their feature owners.

| Track | Required working behavior | Owner | Current state |
| --- | --- | --- | --- |
| P0 | Transaction, fencing and recovery argument | Integration | Written; production histories still need validation |
| P1 | Independent OLAP/OLTP features, optional S3, unchanged ORM protocol | Packaging | Implemented and previously checked; final regression remains |
| P2 | Shared durable pools, indexed primary storage, bounded native work | Storage | Implemented components; production MVCC path still being connected |
| P3 | Streaming exact queries, secondary/composite/covering indexes, online publication | Storage | Index definitions, atomic dual writes and Ready-only selection implemented; online publication and integrated index assertions unfinished |
| P4 | Actual durable Control/TSO/Status and Data consensus over authenticated transport | Packaging and runtime | Real mTLS snapshot and Status service fixtures passed; remote ingress/participant routing and distributed readiness unfinished |
| P5 | Guarded Prepare/Seal/Decide/Resolve, snapshots, exact decisions, schema lifecycle | Storage and runtime | Local real-Raft transaction path passed the ORM program; multi-shard/fault histories and full recovery remain unverified |
| P6 API | All 13 unchanged Python ORM operations through the common RF1/distributed runtime | Runtime | Earlier local parity passed; latest central O01 has an intermittent failure under investigation; separate API/workers unfinished |
| P6 queue | All-ready RAM copies, takeover, original context, retirement, cold abandonment | Runtime | Corrected real retirement/byte-conservation and committed cold replay passed; multi-worker and queued/prepared/ambiguous cold histories unfinished |
| P7 | Verified mounted pools, safe splits/moves, learner catch-up, quorum-aware drain | Packaging | Unfinished |
| P8 backup | Consistent backups, archives/PITR and empty-queue restore | Storage | Unfinished |
| P8 migration | Offline pinned OLAP export and validated OLTP import | Storage | Unfinished |
| P8 deployment | Fixed/manual multi-volume EC2/EKS deployment | Packaging and runtime | Unfinished |
| P9 | Capacity planner and idempotent volume/pool/worker growth claims | Packaging | Unfinished |
| P10 | Measured read/write optimizations without weakening consistency | Storage and packaging | Baseline indexed reads exist; remaining optimization decisions require measurements |
| P11 | Stateful provider adapters, safe autoscaling and spend ceilings | Packaging and runtime | Unfinished |
| P12 local | 29 production acceptance scenarios in seven categories | Runtime runner; all feature owners | Runner implemented; O01 intermittently fails and is being diagnosed; remaining scenarios land with working paths |
| P12 external | Real provider acceptance and matched performance/scaling measurements | Storage O24; packaging O25/provider | Unexecuted; PostgreSQL image/driver prepared; actual matched/provider runs still required |
| Delivery | Final feature/OLAP regressions, reviewable commits and PR | Agents implement/run checks; lead assesses/coordinates | Pending integrated implementation |

The next milestone is one actual server execution path using consensus-backed Control/TSO/Status, guarded MVCC and the replicated RAM queue. Test-only proofs, modeled consensus, fixture authority and the Operation-Raft dependency spike do not satisfy that milestone.

Use [the central acceptance command](../docs/testing/oltp-acceptance.md) for the essential scenarios. Keep existing useful regressions. During implementation run affected assertions through one shared Cargo lane; do not repeat platform matrices, release builds, benchmark captures or presentation work at each component checkpoint. Final feature and regression checks run after stable integration, and repeat only for a new change or failure.
