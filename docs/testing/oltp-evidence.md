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

The [complete feature matrix](evidence/oltp/packaging/full-engine-matrix-summary.json) passed on the same source hash as the quiet process run. Its [raw output](evidence/oltp/packaging/full-engine-matrix.log) contains every target result, dependency-graph assertion results, executable identities, and both invalid compile guards. The separate [graph output](evidence/oltp/packaging/dependency-graphs.log) retains the dependency listings.

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

## Minimum Rust compatibility

The [Docker target guard check](evidence/oltp/docker-target-guards/README.md) executes all four named-stage/binary combinations. Matching engines pass; mismatches exit two and name the required build argument. The matching OLTP Docker definition check also passes. This covers the guard correction, not a complete production-image build.

All four feature selections passed native macOS all-target compilation with Rust 1.94.1, with one test-import correction for OLAP. Both OLTP selections passed on immutable checkpoint `f958c73`. Its default and OLAP-without-S3 checks failed at the inherited `use super as query;` test import. A separate immutable copy changes only that import to `use crate::engine::query;`; both OLAP selections then pass. The [minimum-version evidence](evidence/oltp/msrv-f958c73/README.md) retains original failures, successful reruns, source hashes and commands. These are compilation checks, not test execution, Linux/Docker acceptance or validation of subsequent distributed changes.

## Decision-proof correction

The follow-up correction returns `OutcomeUnknown/Unknown` when an existing mutation's namespace, native read or stored decision JSON cannot be read. The O10/O29 regression commits a table, corrupts the persisted proof encoding, then verifies lookup and replay remain unknown without another write, another UUID or changed table metadata. It also checks corrupt namespace lookup. Pure-read errors retain their separate certainty path. This models invalid proof encoding, not native fsync or SST checksum failure.

The [follow-up library run](evidence/oltp/decision-proof-fix/library.log) passed 78 assertions in 18.82 seconds, including the new regression and in-progress identity/proof helper tests. Its [provenance](evidence/oltp/decision-proof-fix/library-provenance.json) verifies unchanged source SHA-256 `1e8acef80ae5f69fb742ecd808145f9348a5ecfeb396a0b68ba27059e5a98e9e` during execution and records the corrected module's hash. The [scope notes](evidence/oltp/decision-proof-fix/README.md) distinguish this working tree from the isolated fix commit and earlier checkpoint. Identity-stage Clippy passed before later private proof helpers were added; those unfinished helpers are not represented as a current whole-tree lint pass.

## Linux checkpoint and shared queue regression

Immutable commit `251729c` passed all four Rust selections in a Linux container with Rust 1.94.1, a two-CPU quota and 6 GiB RAM. The [Linux evidence](evidence/oltp/linux-251729c/README.md) records 759 default OLAP tests, 757 OLAP-without-S3 tests and 98 tests for each OLTP selection. The ignored live-S3 assertion was not run; each OLTP crash parent invokes its ignored child fixture. The real no-S3 server process and sixteen harness tests also passed. The original pipeline failed at Python discovery with six failures; adding Docker init still left eight failures. Those logs remain recorded. These results do not establish a passing entire Linux pipeline or the later native-budget correction.

The [subsequent Unix-socket correction](evidence/oltp/linux-python-cleanup-fix/README.md) treats Linux connection reset as channel EOF while retaining independent child-exit proof. Signal fixtures use an explicit wait gate. All 179 Python tests then passed on both Linux and macOS, with unchanged recorded Python inputs during each run. This later correction is separate from the original immutable Rust/process capture.

After the shared queue changes, the current default OLAP library/integration suite passed all 759 assertions, with the live-S3 assertion ignored. Its [raw log](evidence/oltp/shared-queue-olap-regression/default-tests.log), [counts](evidence/oltp/shared-queue-olap-regression/summary.json) and [input hashes](evidence/oltp/shared-queue-olap-regression/provenance.json) cover unchanged default/shared Rust inputs during the run. Evolving OLTP-only source was excluded from that hash scope. This is a full default regression run, not a full current OLTP checkpoint.

## Protected pool checkpoint

The [isolated pool checkpoint](evidence/oltp/protected-pool/README.md) overlays four pool files on `c867117` and excludes the evolving authority, typed Raft and MVCC source. All 120 OLTP library tests passed, all-target OLTP Clippy passed with warnings denied, and formatting passed without changes. Its input hashes remained unchanged. The 25 pool assertions include bounded reply aging/cancellation, wide-key paging under declared scratch, atomic scoped log truncation, immutable snapshot membership, and protected consensus metadata with a pending Data apply and held Query slot. Membership apply with full wire arenas and independent reply-decode ownership also passed. These native storage assertions do not establish live typed consensus or distributed transaction behavior.

## Pending evidence

The [shared foundation capture](evidence/oltp/shared-foundation/README.md) passed 112 library tests, four existing Raft integration tests, seven standalone storage integration tests and five real worker-process cases against unchanged combined input SHA-256 `1d20f8d9d9a09f13c0d4ac30594b618e957b739c97bc8fc6e392d7674642685a`. Formatting and all-target OLTP Clippy passed. The [process trace and flow](evidence/oltp/shared-foundation/worker-process/worker-queue-process.html) record both admission barriers, owner/API loss, paged joins, exact 8 MiB transport, predicate depth boundaries and 28 observed child reaps. Its corrected R04 case starts with an acknowledged still-Queued read before killing the final holder, then checks that exact pending identity is abandoned in an explicitly authorized new run.

Native inspection found that database open raises both HIGH and LOW pools to at least one thread. The foundation corrects the original one-thread budget, explicitly configures native job limits, records actual macOS two/eight-thread maintenance observations plus one shared timer, and reserves queried pthread stack/guard defaults separately from explicitly sized Rust stacks. The original functional checkpoints remain recorded with their old settings; their query-thread identities never measured all native threads. The new observations still do not establish complete native RSS/I/O or provider bounds.

The [complete Linux foundation run](evidence/oltp/linux-cb38250/README.md) subsequently passed on immutable commit `cb38250` with Rust 1.94.1, two CPUs and 6 GiB RAM. Default OLAP passed 759 tests, OLAP without S3 passed 757, and each OLTP selection passed 143. The live-S3 assertion remained ignored; OLTP parent tests invoke their two ignored child fixtures. Native server/ORM process, all 179 Python tests and 16 harness tests passed in that same pipeline. Recorded product SHA-256 `56f1e40f10e54274692cafd36da0b01dfdc426221eb652f09b7a502b8dfde763` remained unchanged. This establishes the committed foundation's Linux compatibility, with the individual test scopes above; it does not validate newer uncommitted changes.

The new process cases use fixture authority and pinned reads; the multiplex test registers two endpoints rather than live typed Raft groups. Actual Control/TSO/status services, MVCC transaction transitions, shared RF1/distributed execution, byte-adaptive MVCC scans and status-backed terminal retirement remain required. The pool checkpoint separately verifies reply-arena bounded aging and base paging. The proof verifier remains outside a verified private leader service. Existing general scheduler M03 cases alone do not certify the separate reply allocator.

The single-group consensus dependency spike still logs deterministic application operations. Its local ordered-fence tests do not establish the target rule that cold start applies only prepared committed state without reevaluating abandoned user operations. P4/P5's typed prepare/decision/resolution implementation and its multi-shard/cold-start assertions remain required before production remote execution.

Later shared changes must rerun affected assertions. Distributed worker queue survival with actual authority services, multi-shard MVCC/commit, timestamp services, storage growth, backup/restore, migration and automatic provisioning remain pending implementation or acceptance. Local Linux and minimum-toolchain checks are recorded above. No AWS/EBS/EKS assertion, GitHub CI run or matched PostgreSQL comparison has executed here. Do not infer full scheduler or OLTP contract acceptance from test counts.
