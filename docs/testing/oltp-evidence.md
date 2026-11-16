This ledger records execution for [the OLTP contract](oltp-contract.md) and [implementation plan](../../plans/oltp-engine.md). The implementation is experimental. A local assertion is not evidence of distributed or provider behavior.

## Verified local checkpoint

Worktree: `nabedra/oltp-engine-plan`, based on `0d02e105183e12626e18332bfd35a23ba3266530`. The quiet process run captured complete Cargo/Rust/ORM source SHA-256 `f7f96807c7555fb8fad2982dcb91482f0db5fb97a6b0a5191f3099bcf40349ff`, including untracked implementation files. It verified unchanged source and driver hashes throughout execution. Its recorded Git commit `964ac69` contains the original plan; the implementation was still uncommitted when measured. Later source changes require their own checks.

The native dependency spike compiled `rocksdb 0.25.0` and `librocksdb-sys 0.19.0+11.8.1` on this Intel Mac with Rust 1.99.1 and Apple clang 21.0.0. This does not establish the declared minimum Rust version or Linux/Docker compatibility.

| Executed check | Observation | Scope |
| --- | --- | --- |
| Python unittest discovery | 178 tests passed in 74.892 seconds | Existing Python behavior, larger count decoding, legacy and engine-suffixed native version compatibility |
| Acceptance-harness unit discovery | 16 tests passed in 4.957 seconds | Harness behavior and owned cleanup; no live provider assertion |
| Engine dependency graphs | Default OLAP, OLAP without S3, OLTP, and OLTP with S3 all passed graph assertions | O02 dependency inspection only; not compilation or backup acceptance |
| Storage integration binary | Seven tests passed in 30.07 seconds; one child fixture is ignored during normal discovery and invoked by the crash test | Local subsets of O01/O03/O04/O07/O08/O10/O11/O21 |
| Complete OLTP library suite | 72 tests passed in 12.38 seconds | Official OpenRaft storage suite, shared queue/resource assertions, local fences, snapshot guards, runtime admission, O29 uncertainty models and native ownership/capacity regressions |
| Consensus integration fixtures | Four tests passed in 85.10 seconds | Real mTLS peers and separate stores in one process; quorum/failover, ReadIndex, peer/fence rejection, streamed wide logs and exact partial acknowledgment |
| Immutable RF1 server process | Quiet fixture passed in 20.291 seconds | Thirteen unchanged ORM operations, 110,001-row ORM growth/count across restart, async receipts/replay, atomic conflict, stale IDs, database binding, SIGKILL and CA-verified TLS |
| Default OLAP regression targets | 759 passed and one live-S3 test ignored across 39 targets | Latest complete clean matrix run, after the earlier composite run |
| Additional feature guards/builds | OLAP without S3 checked cleanly; OLTP with S3 built and reported its engine; both-engine and neither-engine builds failed at the explicit compile guard | O02 selections; S3 backup remains unimplemented |
| Formatting and OLTP lint | Formatting passed; OLTP all-target Clippy passed with warnings denied | Default Clippy's inherited findings are described separately |

The [complete feature matrix](evidence/oltp/packaging/full-engine-matrix-summary.json) passed on the same source hash as the quiet process run. Its [raw output](evidence/oltp/packaging/full-engine-matrix.log) contains every target result, all four dependency graphs and executable identities, and both invalid compile guards.

| Features | Passed | Ignored |
| --- | --- | --- |
| Default OLAP with S3 | 759 | One explicitly authorized live-S3 fixture, not run |
| OLAP without S3 | 757 | None |
| OLTP without S3 | 97 | One crash child fixture, invoked by its parent assertion |
| OLTP with S3 | 97 | One crash child fixture, invoked by its parent assertion |

The [full Python output](evidence/oltp/packaging/full-python.log) and [harness output](evidence/oltp/packaging/scripts-unit.log) retain the 178 and 16 passing results. Default Clippy remains failing with 23 inherited errors after shared cleanup, compared with 27 in the earlier audit; [the final comparison](evidence/oltp/packaging/default-clippy-checkpoint-comparison.json) found no added diagnostic classes/files. The [failed check output](evidence/oltp/packaging/default-clippy-checkpoint.log) is retained. This failure is not represented as a passing required check.

The storage growth assertion inserted 110,001 rows with 84,480,768 raw string bytes, closed and reopened the store, checked exact count 110,001, checked explicit count limit 100,001, read a key above the former limit, and checked a bounded range. An unbounded oversized collection failed explicitly. Insertion took 21.798 seconds in this unoptimized development build. This single fixture is a functional limit regression, not a repeated throughput benchmark. It does not establish shard splits, additional volumes, or O21 in full.

Storage assertions also checked all thirteen operations, signed integer extrema, UTF-8 key boundaries, signed float zero, descending sort ties, a seeded serial query oracle, atomic duplicate-key rejection, exact durable replay and database binding. A child process was killed after acknowledging a synchronous durable write, then the parent reopened its data and decision. Process crash recovery does not establish power-loss/fsync correctness.

The shared build environment was:

```sh
export CARGO_TARGET_DIR=/Users/antoniopenapena/Documents/ResearchProjects/AI/nabedra-database/server/target
export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2
```

Representative reproduction commands, with exact executed output retained in the linked artifacts:

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/python/src python3.11 -m unittest discover -s orm/python/tests -v
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/python/src python3.11 -m unittest discover -s scripts -p 'test_*.py' -v
python3.11 scripts/check_engine_features.py --dependencies-only
cargo fmt --manifest-path server/Cargo.toml --all -- --check
cargo clippy --locked --manifest-path server/Cargo.toml --no-default-features --features oltp --all-targets -- -D warnings
cargo test --locked --manifest-path server/Cargo.toml --no-default-features --features oltp --lib
cargo test --locked --manifest-path server/Cargo.toml --no-default-features --features oltp --test oltp_storage -- --nocapture
cargo test --locked --manifest-path server/Cargo.toml --no-default-features --features oltp --test oltp_replication -- --test-threads=1
cargo test --locked --manifest-path server/Cargo.toml --lib --tests
cargo test --locked --manifest-path server/Cargo.toml --test startup --test storage --test worker_pool
```

See [packaging commands and qualifications](evidence/oltp/packaging/README.md), [library output](evidence/oltp/packaging/oltp-final-lib.log), [storage output](evidence/oltp/rf1-storage-integration.log), and [consensus output](evidence/oltp/packaging/consensus-fixtures.log). The three-peer debug fixture declares a 512 MiB task budget, roughly 8 MiB transport batches, five-second heartbeat and 15-to-20-second elections. These settings make its wide workload reproducible; they are not production latency recommendations.

The quiet RF1 executable has SHA-256 `61f458b746e4e9fc3bd840cbb8e726a07a5b742ca3a45c38fa8e0f9257d21afe`. Its [process log](evidence/oltp/quiet-rf1/rf1-process.log) and [measurements/configuration](evidence/oltp/quiet-rf1/rf1-process.json) record driver SHA-256 `cda5a7aa58752f204791fe187f366958a11e49c2e3d5992f6ae61a65019bbeec`, exact arguments and environment. The process fixture confirms that committed mutations and an exact old-run mutation receipt survive SIGKILL, restart creates a new run, and an old completed query result is unavailable. Queued-mutation abandonment is the separate deterministic runtime assertion. The fixture also checks actual ORM counts 110,001 and limit 100,001 across restart, and rejects an oversized thread-budget override before disk initialization/readiness.

The [rendered process evidence](evidence/oltp/quiet-rf1/rf1-process.html), [latency figure](evidence/oltp/quiet-rf1/rf1-orm-latency.svg), and [renderer provenance](evidence/oltp/quiet-rf1/rf1-plot-provenance.json) use 30 warm-up calls and three repetitions of 90 sequential ORM calls on a three-row query table. The separate growth table was dropped before warm-up. Repository compilation/tests were paused; recorded process checks found no Rust/C++ compilers, while host load remained measurable. These are development loopback completion samples, without measured CPU quota or cache-hit rates. They do not establish B01/B02, cold reads, throughput scaling or a PostgreSQL comparison. Earlier contended results remain under `evidence/oltp/rf1-*` as initial observations and are not a before/after speedup comparison.

O22/C07's deterministic writer test holds only asynchronous writer ownership while two real mutations wait, proves they own no scheduler CPU grants, completes an unrelated read and receipt, then settles both writes once. O05/F03's native-cancellation test retains CPU/memory/native ownership after the awaiter is cancelled until native cleanup finishes. O29 models a lost observation after an actual synchronous write and stale live view, preserves the original UUID across recovery, blocks execution through every alias of the uncertain pool, and retains accepted uncertain work as Reconciling through result expiry. These tests do not inject real native fsync failure or establish complete native/RSS accounting.

The initial OLAP run found an inaccessible tuple field after extracting shared storage types, which was corrected to use the existing accessor. Its startup assertion then required the intentional new engine suffix in `--version`. The corrected startup target and remaining storage/worker-pool targets passed, and the later full matrix supersedes that earlier composite evidence. The ignored live-S3 fixture remains unexecuted. [Initial output](evidence/oltp/packaging/default-initial.log), [corrected targets](evidence/oltp/packaging/default-final-targets.log), and [feature guard output](evidence/oltp/packaging/feature-compile-guards.log) retain the observed failures and passes.

## Pending evidence

Known follow-up at this measured snapshot: native read/namespace errors or corrupt JSON while looking up an existing mutation decision already prevent recomputation, but currently classify proof unavailability as `not_applied`. O10/O29 require `unknown` for those paths. A narrow correction and corrupt-decision regression are queued immediately after the immutable checkpoint; the current results do not establish that case. Ordinary pre-publication schema/row validation certainty is a different path.

The single-group consensus dependency spike still logs deterministic application operations. Its local ordered-fence tests do not establish the target rule that cold start applies only prepared committed state without reevaluating abandoned user operations. P4/P5's typed prepare/decision/resolution implementation and its multi-shard/cold-start assertions remain required before production remote execution.

Later shared changes must rerun affected assertions. Distributed worker queue survival, multi-shard MVCC/commit, timestamp services, storage growth, backup/restore, migration and automatic provisioning remain pending implementation or acceptance. No AWS/EBS/EKS assertion, Linux/MSRV CI run or matched PostgreSQL comparison has executed here. Do not infer full scheduler or OLTP contract acceptance from test counts.
