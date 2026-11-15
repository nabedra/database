# PW-E02 live acceptance

PASS on 2026-11-10. Ran the authorized command exactly once from the repository root on the shared main checkout:

```sh
PYTHONDONTWRITEBYTECODE=1 orm/.venv/bin/python scripts/acceptance_partitions.py --binary server/target/debug/meshdb-server
```

The runner exited 0. Its sanitized report records 53/53 passed checks, no cancellation, and 158.827 seconds elapsed. It used `meshdb-testing` in `us-east-1`, initially with 2 workers and then a same-prefix restart with 3 workers. Both configurations used `64 KB`, reported as 64,000 bytes.

Checks passed for actual Parquet files and catalog contents, CRUD across key ranges, global queries including descending ties/limit/projection and counts, untouched partition references, conflicting-batch atomicity without publication, no-op preservation, restart identity and persistence, and a post-restart upsert.

| Checkpoint | Current partitions | Maximum actual file bytes |
| --- | ---: | ---: |
| empty | 0 | 0 |
| inserted | 8 | 50,184 |
| mutated | 8 | 50,184 |
| after_conflict | 8 | 50,184 |
| after_noops | 8 | 50,184 |
| restarted | 8 | 50,184 |
| post_restart_mutation | 8 | 50,184 |

The empty checkpoint has no files. Every nonempty checkpoint's maximum actual file size was below the configured target. `graceful_stop_before_restart` passed, and both recorded `graceful_process_stop` checks passed, confirming exit-code-0 stop checks.

Retained fixtures:

- Prefix: `test-10-partition-workers-a72b4fca72a1/`
- Table: `partition_rows`, ID `344bbe59-e7a6-44fa-91bf-8597c191660c`, final row count 96.
- Sanitized report: `artifacts/acceptance-partitions-23a2798911364544bb80212c3855a8e5.json`.

Only this tracked note was written by the test agent; the runner wrote the ignored sanitized report. No additional acceptance runs, Cargo checks, AWS commands, retries, or deletes were performed. No unresolved acceptance failures were reported. These facts establish this acceptance scenario's result; they do not establish benchmark performance or scaling.
