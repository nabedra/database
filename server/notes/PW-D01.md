# PW-D01 documentation update

Documentation-only assignment in the shared main checkout. No agents, branches,
worktrees, commits, pushes, Cargo commands, AWS calls or server launches. No
credentials or dotenv files were read. The coordinator's plan and all code and
tests were left untouched. Applied the unslop skill.

## Changed files

- [README.md](../docs/overview.md): current partition behavior, exact serve command,
  size syntax and limits, local workers, compatibility, prepared acceptance
  command and current rubric link.
- [server/README.md](../docs/reference.md): CLI defaults and aliases, persistent worker
  routing and bounded queues, partition paths and metadata, changed-file writes,
  sequential uploads and atomic publication, lazy legacy migration and retention.
- [TUTORIAL.md](../TUTORIAL.md): existing start/restart commands use four workers
  and a 64 KB target; storage and limit explanations match the implementation.
- [orm/README.md](../../orm/python/docs/reference.md): replace full-table scan/rewrite claims with
  routed keyed operations, global query semantics and unchanged aggregate limits.
- [server/notes/PW-D01.md](PW-D01.md): this evidence record.

The guides keep the single-process-per-bucket/prefix restriction. They distinguish
64 MB of closed Parquet bytes from the retained 64 MiB decoded table limit,
explain singleton soft-target overflow and whole-mutation hard-cap failure, and
state that changed configuration does not repack untouched files. Old plan-v0
specifications remain unchanged and linked as historical baselines.

## Implementation evidence

Read PW-P01 through PW-P04, PW-W01, PW-W01b, PW-W02 through PW-W04 and the E01
preparation note. Cross-checked final config/main, partition size parsing, worker
pool, engine partition loading/dispatch, keyed reads and mutations, query
reduction, snapshot partition encoding, manifest compatibility and publication.
These show original-manifest-index modulo N routing, worker-owned fetch and data
work, coordinator reduction/publication, separate request/partition budgets,
sequential uploads, empty manifests and mixed Legacy/Partitioned v2 catalogs.
No throughput, remote-worker or scale result is claimed.

## Offline validation

- An inline check run with `PYTHONDONTWRITEBYTECODE=1 orm/.venv/bin/python`
  validated all 53 local links in the four guides, including heading fragments.
  It parsed all 12 Python examples with `ast.parse`, checked all 21 shell blocks
  with `zsh -n` without executing them, and checked shell tokenization for all
  four copies of the exact serve command. All passed.
- The first invocation of that checker used system `python3`, which is Python
  3.6 and rejected the checker's `subprocess.run(text=...)` argument. The rerun
  used the repository virtual environment and passed; no guide change was needed.
- `PYTHONDONTWRITEBYTECODE=1 orm/.venv/bin/python scripts/acceptance_partitions.py --help`
  passed offline, showing `--binary` and `--report`. This path does not load
  private configuration or launch the binary. Server flags and size examples
  were checked against source and P01 evidence, without running the server CLI.
- Targeted text inspection found the removed whole-table scan/rewrite and
  request-only-worker claims only in explicitly historical descriptions.
- `git diff --check -- README.md server/README.md TUTORIAL.md orm/README.md server/notes/PW-D01.md`
  passed.

No unit tests were added or run for this documentation change. Live E02 acceptance
runs independently. Its result was not confirmed during this assignment; the
guides document the prepared command and mark the live result pending. The
coordinator should reconcile that status with E02 evidence when available.
