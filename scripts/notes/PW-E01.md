# PW-E01 partition acceptance preparation

Prepared one scenario. No AWS calls, actual Rust server launches, Cargo commands,
repository .env reads, secret inspection, agents, branch/worktree changes,
commits or pushes occurred. Existing guard tests use synthetic credentials and
synthetic temporary dotenv files only. Rust, ORM, plans and other notes were
not edited.

## Owned files

- New `scripts/acceptance_partitions.py`.
- `scripts/server_process.py`: optional `workers=4` and `partition_size=None`.
  Workers reject booleans, non-integers and non-positive values before setup.
  Configured size is passed as one argument after `--partition_size`. Existing
  callers retain their default command and TLS/lifecycle ownership.
- `scripts/acceptance_concurrency.py`: reuse its bounded read-only reader for
  the fixed partition-workers prefix. Catalog references are now lists, with
  support for v1 snapshots, v2 legacy snapshots and v2 partitioned manifests,
  including empty manifests. Validate canonical paths and u32 ordinals, table
  identities, current object presence, metadata byte sizes, positive partition
  counts, contiguous range endpoints and row/payload aggregate limits.
  Retention assertions flatten reference lists. The historical main-file lower
  bound is five populated writes, without depending on empty-file creation.
- Existing `scripts/test_server_process.py`, `scripts/test_acceptance_support.py`
  and `scripts/test_acceptance_cancellation.py`: process/configuration and one
  catalog shape group, plus report-setup cancellation coverage for the new runner.
- This note.

## Prepared live command, not run

From the repository root after coordinator review and live authorization:

```sh
PYTHONDONTWRITEBYTECODE=1 orm/.venv/bin/python scripts/acceptance_partitions.py --binary server/target/debug/meshdb-server
```

Requires the existing binary, installed public ORM and AWS CLI. Only `--binary`
and `--report` are configurable. `--help` and import never load configuration.
Runtime alone calls acceptance_support.load_config and its private namespace
and environment guards. No AWS command or bucket/prefix override is exposed.
Allocation number 10 was unused; existing runners allocate 1 through 9.
The scenario accepts only meshdb-testing in us-east-1 and allocates exactly
`test-10-partition-workers-<run-id>/`. The same allocated namespace is reused
on restart, with no second allocation.

## Scenario

Launch the actual binary with two workers and `--partition_size '64 KB'`.
Create one empty int-key table and verify a v2 empty partition manifest and
zero Parquet objects. Insert 96 rows in reverse key order, each with distinct
deterministic 4096-character text, a float score with four repeated values,
and a boolean. The fixture is about 400 KB, within ordinary request, batch,
result and table limits. Require at least five current files, greater than both
worker counts. Each initial file must contain at least four rows, so score ties
span partitions.

At storage checkpoints inspect the live catalog, listing, and every current
object. Require canonical table/commit/ordinal references, exact listed and
manifest byte lengths, full bounded GET length, PAR1 header/footer, a sane
footer length, each actual file at most 64000 bytes and at most the 64000000
hard cap, exact total row count and exact fixture payload accounting. Empty
creation requires no Parquet object. The existing reader limits listing to
100 objects without pagination and catalog/ranges to 1 MiB plus one sentinel
byte; these fixtures remain below those bounds. Child AWS stdout stays private,
stderr is discarded, argv is a list, and config sets AWS_MAX_ATTEMPTS=1.
Only list-objects-v2 and bounded get-object are used, with 20-second subprocess
limits. Temporary local range files are private and removed after reads.

Through public ORM calls, compare full rows, descending float order with the
ascending primary-key tie order, limit 17 and projection excluding both the
sort field and key. Verify limited count as well. Get keys 2, 50 and 90, update
the low range, upsert an existing middle-range row, and delete the high-range
row. Compare all expected rows again, inspect all current files, and require
unaffected ranges to retain their exact references. Historical objects remain
listed. Submit a new row followed by a conflicting existing key in one batch;
require ConflictError/conflict/409/not_applied, no new row, exact logical data,
byte-identical catalog and identical object listing/reference metadata. Empty
batch and missing delete must also preserve catalog and objects.

Stop gracefully, restart the same namespace with three workers and the same
size, rediscover the same table ID/schema/key, and compare rows, query results,
catalog bytes and object metadata. Upsert a new row after restart and repeat
logical and storage checks. Retain every remote object. No delete cleanup,
mutation retry/replay, benchmark or timing-based parallelism assertion exists.
Deterministic Rust tests establish worker execution; this scenario checks the
actual-server/S3 behavior only.

## Report and shutdown policy

Default report is `artifacts/acceptance-partitions-<uuid>.json`. Reserve it with
exclusive creation before loading configuration; refuse existing files.
Save failed or cancelled partial reports, including an interrupted report
setup when a fresh destination can still be reserved. Exit codes are 0, 1 and
130 for pass, failure and cancellation. Reports contain fixed bucket/region,
allocated prefix, fixture identity/count/state, labels/status and allowlisted
failure types, configured worker counts/target, per-checkpoint storage counts,
sizes, relative references and Parquet framing evidence. Rows, raw catalog,
exception messages, child outputs, tokens, environment and credentials are
never serialized. Elapsed seconds are diagnostic only.

Both ServerProcess objects remain owned through final cleanup and are stopped
and reaped on success, failure and Ctrl-C. SIGINT is deferred during shutdown
and report writing; forced/nonzero shutdown fails acceptance. No TLS material
is created by this scenario; existing ServerProcess TLS behavior is unchanged.
Only local temporary range files are cleaned up. S3 fixtures and reports remain.

## Local evidence

The new process arguments and catalog shape guard first failed because the
requested interfaces did not exist. The extended cancellation guard caught an
unhandled KeyboardInterrupt during new-runner report setup; the runner now
saves the cancellation report without loading live dependencies.

Passed:

```sh
PYTHONDONTWRITEBYTECODE=1 orm/.venv/bin/python -m unittest discover -s scripts -p 'test_*.py'
PYTHONDONTWRITEBYTECODE=1 orm/.venv/bin/python scripts/acceptance_partitions.py --help
PYTHONDONTWRITEBYTECODE=1 orm/.venv/bin/python scripts/acceptance_concurrency.py --help
```

Six guard tests passed, including real local fake-server process restart,
configured two/three-worker argv, defaults, failed startup cleanup, synthetic
configuration/namespace safety, v1/v2/empty manifest shapes and metadata-size
rejection, cancellation and existing TLS cleanup ownership. AST parsing passed
for acceptance_partitions, acceptance_concurrency and server_process. A fresh
Python inert-import check confirmed acceptance_support, server_process and
meshdb remained unloaded after importing acceptance_partitions; its generated
text fixture has exactly 4096 characters.

Live binary compatibility, S3 permissions, actual object partition counts and
sizes, end-to-end mutations/restart and live cancellation remain unverified.
Preparation is complete; coordinator review and live execution are deferred.

## Review correction

Specification review passed. Quality review found that the inspection helper accepted reversed or mistyped range endpoints. The coordinator added four cases to the existing CatalogShapeGuard for reversed ranges, string endpoints in an int64 table, Boolean endpoints and out-of-range int64 endpoints. All four failed before the correction. The inspector now checks endpoint types against the table schema, signed-int64 bounds and strict ordering for finite ranges. `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=scripts orm/.venv/bin/python -m unittest test_acceptance_support.CatalogShapeGuard` and `git diff --check` pass afterward. Focused quality re-review is pending. Live execution is already authorized by the user's requested workflow and will follow successful review.
