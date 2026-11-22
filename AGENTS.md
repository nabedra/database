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

# OLTP direct path

For OLTP request-path work, read
[the direct path plan](plans/oltp-direct-path.md) first. The user decided on
2026-11-19 that synchronous single-key OLTP operations skip the replicated
queue. The owning range's leader commits each one in a single Data log
entry, which may carry other requests' commits to the same range, and a retry with the same request ID and body returns the original
result. Request ID reuse is detected per table and key on this path. The path
is on by default, and `direct_oltp: false` restores the queued path.

The queue rules above still apply to everything that stays queued: OLAP,
`POST /v1/requests` and receipts, schema changes, and cross-range batches.
Where the direct path plan conflicts with the scheduling plan, the shard
refactor plan, or scenarios Q03, Q05 and O23 for an eligible operation, the
direct path plan wins. The plan describes target behavior and does not claim
the runtime implements it.

# ORM parity

For every ORM change and any database change that affects ORM callers, follow the
[ORM parity checks](docs/testing/orm-parity.md). Apply shared changes to every
supported language in the same PR and keep their package versions in sync.

# CI on GitHub Actions

Run heavy tests and checks on GitHub Actions. A push or commit alone starts
no run. Agents dispatch runs pinned to a commit. Later pushes and other
agents' dispatches cannot cancel them. The pull-request gate to `main`
remains. The scheduled nightly workflow pins its checks to the SHA of
`main` when it starts. Follow [GitHub Actions for checks](.agents/skills/code-review/references/gh-actions.md)
for dispatching, evidence and the quarantine. When agents work in parallel,
each hot file has one owner per wave
([hot-file rule](.agents/skills/nabedra-change/references/subagent-driven-development.md#prepare-the-work)).
These rules apply to every task and workflow in this repository.

# Testing system

Each module's guarantees are in a register in `docs/testing/guarantees/`.
`docs/testing/modules.toml` maps paths to modules and guarantees, each check
is defined in `docs/testing/checks/`, and `docs/testing/coverage-matrix.md`
shows the state of every cell. Follow the [evidence rules](.agents/skills/code-review/references/evidence.md)
and the [verification rules](.agents/skills/code-review/SKILL.md#verification-rules).
[docs/testing/README.md](docs/testing/README.md) is the overview and quickstart.

A guarantee is verified only by attested evidence on the SHA being shipped.
Simulation evidence is labelled as a fault model and does not replace the
real-environment checks the register requires.

Real-environment compute runs in Docker or kind on GitHub Actions runners or
developer machines. EC2, EKS and other cloud compute are out of scope. Report
checks that require them as `not-run: cloud-deferred`. S3 checks use only the
dedicated test bucket under `runs/<request_id>/` and report
`not-run: s3-unconfigured` when `NABEDRA_TEST_S3_BUCKET` is unset.
