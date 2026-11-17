Implementation continues on `nabedra/oltp-engine-plan` in the existing design worktree. The full module is **not finished**. This is the finite implementation checklist, not a claim of acceptance. Historical build and component results remain in [the evidence ledger](../docs/testing/oltp-evidence.md).

| Track | Required working behavior | Owner | Current state |
| --- | --- | --- | --- |
| P0 | Transaction, fencing and recovery argument | Integration | Written; production histories still need validation |
| P1 | Independent OLAP/OLTP features, optional S3, unchanged ORM protocol | Packaging | Implemented and previously checked; final regression remains |
| P2 | Shared durable pools, indexed primary storage, bounded native work | Storage | Implemented components; production MVCC path still being connected |
| P3 | Streaming exact queries, secondary/composite/covering indexes, online publication | Storage | Primary standalone query path exists; production planner/index path unfinished |
| P4 | Actual durable Control/TSO/Status and Data consensus over authenticated transport | Packaging and runtime | State machines and typed host exist; live service construction/integration unfinished |
| P5 | Guarded Prepare/Seal/Decide/Resolve, snapshots, exact decisions, schema lifecycle | Storage and runtime | Native MVCC transitions exist; real Status proofs, lifecycle guards and coordinator unfinished |
| P6 API | All 13 unchanged Python ORM operations through the common RF1/distributed runtime | Runtime | Standalone legacy path works; common production path unfinished |
| P6 queue | All-ready RAM copies, takeover, original context, retirement, cold abandonment | Runtime | Process/component helpers exist; actual authority integration and retirement unfinished |
| P7 | Verified mounted pools, safe splits/moves, learner catch-up, quorum-aware drain | Packaging | Unfinished |
| P8 backup | Consistent backups, archives/PITR and empty-queue restore | Storage | Unfinished |
| P8 migration | Offline pinned OLAP export and validated OLTP import | Storage | Unfinished |
| P8 deployment | Fixed/manual multi-volume EC2/EKS deployment | Packaging and runtime | Unfinished |
| P9 | Capacity planner and idempotent volume/pool/worker growth claims | Packaging | Unfinished |
| P10 | Measured read/write optimizations without weakening consistency | Integration | Baseline indexed reads exist; remaining optimization decisions require measurements |
| P11 | Stateful provider adapters, safe autoscaling and spend ceilings | Packaging and runtime | Unfinished |
| P12 local | 29 production acceptance scenarios in seven categories | Integration | Central runner being added; individual scenarios land with working paths |
| P12 external | Real provider acceptance and matched performance/scaling measurements | Integration | Unexecuted; requires configured provider/PostgreSQL environments |
| Delivery | Final feature/OLAP regressions, reviewable commits and PR | Integration | Pending integrated implementation |

The next milestone is one actual server execution path using consensus-backed Control/TSO/Status, guarded MVCC and the replicated RAM queue. Test-only proofs, modeled consensus, fixture authority and the Operation-Raft dependency spike do not satisfy that milestone.

Use [the central acceptance command](../docs/testing/oltp-acceptance.md) for the essential scenarios. Keep existing useful regressions. During implementation run affected assertions through one shared Cargo lane; do not repeat platform matrices, release builds, benchmark captures or presentation work at each component checkpoint. Final feature and regression checks run after stable integration, and repeat only for a new change or failure.
