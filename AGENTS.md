# Worker execution requirements

For changes to request admission, queues, scheduling, workers, partition memory,
mutation publication, recovery, or autoscaling, read
[the scheduling plan](plans/resource-aware-scheduling.md) and
[the reusable test contract](docs/testing/scheduler-contract.md).

These record the user's requirements from 2026-11-15. They describe the target
behavior; they do not claim that the current runtime implements it. Configuration
and protocol examples in the plan are proposals until approved.

Preserve the test scenarios in future relevant work. Cite their identifiers in
implementation plans and test evidence. Add regression cases for new failures.
Do not report these scenarios as passing without executing their assertions.

The queue lives in replicated worker memory. Accepted work survives individual
API/worker failures while a worker retains its complete request record. After
every worker stops, abandon outstanding requests instead of replaying them on
restart. Committed mutations remain durable. This supersedes the earlier
suggestion to persist the request queue across a complete cluster shutdown.

# ORM parity

For every ORM change and any database change that affects ORM callers, follow the
[ORM parity checks](docs/testing/orm-parity.md). Apply shared changes to every
supported language in the same PR and keep their package versions in sync.
