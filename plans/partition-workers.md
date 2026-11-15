# Partition workers implementation and testing rubric

Store tables in bounded Parquet partitions and distribute each operation's partition work across the configured workers.

Complete, including the requested follow-up. The default and per-partition hard maximum are 64 MiB, or 67,108,864 bytes. Follow-up checks and both independent reviews passed. Changes remain uncommitted on `main`.

The coordinator plans, delegates and records evidence. Every child uses `gpt-6.1-sol` with `reasoningEffort=high` and receives one bounded task. Work stays on `main` in the shared checkout, as requested in the previous implementation thread. No branches, worktrees, commits or pushes are needed. Assign overlapping edits sequentially. The user's minimal-testing instruction takes precedence over per-function test requirements in generic skills.

## Required behavior

- `meshdb-server serve --workers 4 --partition_size '64 KB'` configures partition execution and a byte-size target. Also accept conventional `--partition-size`.
- Sizes require a unit and accept case-insensitive `B`, `KB`, `MB`, `KiB`, `MiB`, with optional whitespace between number and unit. Decimal units use powers of 1000; binary units use powers of 1024. Reject missing units, malformed input, zero, overflow and sizes above 64 MiB, meaning 67,108,864 bytes.
- Measure closed Parquet file bytes, including metadata. Aim at or below the configured target, with a hard maximum of 67,108,864 bytes per new partition. A singleton may exceed a smaller target but never the hard cap; empty tables have no partition files.
- Read and mutate partitions through a bounded shared worker pool. Partition count and worker count are independent; workers process further partitions when there are more partitions than workers.
- Preserve global query ordering, key tie-breaking, projection, limit and count semantics when combining worker results.
- Preserve typed keys, duplicate rejection, atomic batches, immutable table IDs, captured read snapshots, conditional catalog publication, uncertain outcomes and shutdown draining.
- Workers finish partition preparation and the coordinator uploads all new files before a single catalog commit makes the operation visible. Failed preparation or upload must not partially publish a table.
- Existing v0 data remains readable, with a documented migration path to partition manifests. Never silently discard or overwrite existing data.
- `--workers N` creates N dedicated persistent local worker threads, each with a stable worker ID. The assigned worker retrieves its Parquet object and performs its operation on that thread. The coordinator combines results and commits. Remote machines are future work. Request concurrency and partition execution use separate budgets to avoid nested acquisition deadlocks.

## Mini-task queue

- [x] A01. Inspect the current catalog, Parquet codec and commit path; propose concrete partition metadata, compatibility and file-size contracts. Read-only design task.
- [x] A02. Inspect execution, queries and mutation flow; propose bounded partition dispatch and a minimal proof of parallel execution. Read-only design task.
- [x] A03. Adapt the worker execution API to the user's explicit one-worker-per-thread requirement. Read-only design task.
- [x] P01. Implement unit-bearing size parsing, CLI aliases and validated defaults with representative parser checks.
- [x] P02. Add validated partition metadata and legacy-catalog reading with malformed-manifest checks.
- [x] P03. Implement splitting by actual encoded Parquet size, including indivisible rows and hard-limit checks.
- [x] P04. Extend conditional publication to an atomic set of partition objects, with upload-failure and reconciliation checks.
- [x] W01. Add bounded partition dispatch with owned work and deterministic failure/drain behavior.
- [x] W01b. Inject the shared worker pool and partition size into Engine and wire startup/shutdown ownership, preserving existing test fixtures.
- [x] W02. Distribute partition reads and query evaluation; combine results with global query semantics.
- [x] W03. Route keyed mutations and batch inserts to partition tasks while preserving global validation and atomicity.
- [x] W04. Wire configured workers and partition size through startup, create/drop and lifecycle behavior.
- [x] E01. Prepare one focused actual-server acceptance scenario for partition size, CRUD/query results, restart and worker counts.
- [x] E02. Run the new acceptance scenario against the supplied S3 bucket and inspect actual partition objects.
- [x] D01. Update CLI, storage and worker documentation and the tutorial where behavior changed.
- [x] V01. Complete independent specification review and resolve actionable findings.
- [x] V02. Complete independent quality review after specification review passes.
- [x] V03. Run the necessary final checks and reconcile all completion boxes with evidence.
- [x] V03a. Run Rust formatting, strict all-target Clippy and the current binary build, reusing the completed 93-test evidence.
- [x] F01. Raise the default and hard cap to 64 MiB; update affected boundaries, acceptance inspection and current guides, and run focused checks.
- [x] F02. Independently review the cap follow-up for specification and quality, then reconcile evidence.

Each implementation task received separate specification and quality review. Review findings were corrected and re-reviewed. Completion boxes correspond to the evidence below and the linked task notes.

## Validation policy

Use small tests for meaningful behavior and failure modes, preferably real codec/engine paths and the existing conditional-write storage fake. Use deterministic barriers or channels to prove overlapping partition execution and worker limits; timing alone cannot prove concurrency. Do not create a duplicate suite for every helper or repeat full checks without a change or unresolved concern.

Run Cargo commands sequentially with the existing `server/target` and `CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`. Reuse existing ORM and lifecycle coverage. Final checks include formatting, all-target strict Clippy and the server suite, plus Python checks affected by this work.

Live acceptance uses only `meshdb-testing` in `us-east-1`, privately loaded through the existing helpers. Allocate fresh `test-<number>-<name>-<run-id>/` prefixes, retain fixtures and reports, never print credentials, and never delete existing test data. Record exact commands, sanitized reports and allocated prefixes.

## Evidence

- A01-A03 established typed partition ranges, atomic publication, and the user's dedicated-local-thread contract before dependent implementation.
- P01 passed parser/CLI/config checks and both reviews. Unit syntax, SI/IEC arithmetic, aliases and bounds are recorded in [PW-P01](../server/notes/PW-P01.md).
- P02 passed seven storage groups and both reviews after rejecting positional arrays for file/range objects. Version-1 compatibility, version-2 manifests and typed routing are recorded in [PW-P02](../server/notes/PW-P02.md).
- P03 passed nine snapshot groups and both reviews, including real hard-cap splitting and singleton rejection. See [PW-P03](../server/notes/PW-P03.md).
- P04 passed ten commit groups and both reviews, covering exact upload membership, retained references, later-upload failure and catalog reconciliation. See [PW-P04](../server/notes/PW-P04.md).
- W01 passed two deterministic worker groups and both reviews. They prove native thread identity, overlap, reuse, queue bounds, panic recovery and drainage, including the corrected unpolled-reservation hang. See [PW-W01](../server/notes/PW-W01.md).
- W01b passed runtime/startup checks and both reviews. Engine clones share the pool, and requests drain before thread shutdown. See [PW-W01b](../server/notes/PW-W01b.md).
- W02 passed four engine-read and five query groups and both reviews. Captured snapshots, keyed routing, global query semantics and sibling drainage are covered. See [PW-W02](../server/notes/PW-W02.md).
- W03 passed eighteen targeted groups and both reviews. Atomic batches, targeted rewrites, retained objects, lazy migration and aggregate accounting are covered. See [PW-W03](../server/notes/PW-W03.md).
- W04 passed both reviews and completed all 93 Rust tests across suite/remaining-target/repair runs. The repaired fixtures address shared-worker gate sequencing and unsupported catalog version 3. New empty tables publish only metadata. See [PW-W04](../server/notes/PW-W04.md).
- E01 passed six local Python guards and both reviews. The coordinator fixed typed/order range inspection, observed failing cases, passed the existing guard and obtained independent re-review. See [PW-E01](../scripts/notes/PW-E01.md).
- E02 passed once with exit 0 and all 53 live checks. The actual server restarted from two to three workers with a 64,000-byte target. Each nonempty checkpoint had eight partitions; the largest file was 50,184 bytes. Both graceful process stops passed. Fixtures remain in `test-10-partition-workers-a72b4fca72a1/`. See [PW-E02](../scripts/notes/PW-E02.md) and the ignored report `artifacts/acceptance-partitions-23a2798911364544bb80212c3855a8e5.json`.
- D01 passed example/link checks and independent final documentation specification and quality reviews. See [PW-D01](../server/notes/PW-D01.md).
- V01 and V02 passed final integration and documentation audits with no material findings. See [PW-V01](../server/notes/PW-V01.md) and [PW-V02](../server/notes/PW-V02.md). Historical pending-E02 statements are superseded by its completed run.
- V03a passed full formatting, strict all-target Clippy and the final binary build after small behavior-preserving lint corrections. See [PW-V03a](../server/notes/PW-V03a.md).
- V03 reconciled all evidence, the built binary, retained live report, ignored private/generated paths and workspace status. See [PW-V03](../server/notes/PW-V03.md). Every box is complete; no requested task remains open.

## Agreed metadata contract

The F01 follow-up supersedes earlier decimal-cap descriptions in historical task notes. Three parser tests, 34 targeted Rust integration tests, the Python catalog guard, formatting, strict all-target Clippy and rebuild passed for the 64 MiB change. Both specification and quality reviews passed without findings. See [PW-F01](../server/notes/PW-F01.md). The earlier live run retains its original 64 KB target and is not presented as a new-cap live test.

Version 1 remains readable with its canonical snapshot paths and 80 MiB encoded read allowance. New catalogs use version 2 and permit legacy and partitioned table entries. A legacy table migrates on its next actual data mutation; no-ops and reads preserve it. Version 2 partition entries record immutable object key, lower-inclusive/upper-exclusive typed key range, row count, actual encoded byte size and decoded payload byte size. Nonempty manifests cover the key domain contiguously, contain positive-row files, and have unique canonical object keys. Empty manifests contain zero rows. Validate row/payload totals against existing table limits, and file sizes against the new hard cap. Publication may adjust the routing bounds of retained objects but must preserve their physical row/byte metadata.
