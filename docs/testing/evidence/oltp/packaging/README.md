This directory records packaging checks on macOS x86_64 with the installed stable Rust toolchain, rustc/Cargo 1.99.1. It does not establish Linux, the declared Rust 1.94.1 minimum, container builds, a clean installation, live S3 backup, or distributed deployment acceptance. The Ubuntu/MSRV matrix is defined in `.github/workflows/engine-matrix.yml` and has not been run here.

The four dependency graphs passed `python3.11 scripts/check_engine_features.py --dependencies-only`. The script checks OLAP versus RocksDB/OpenRaft selection and rejects AWS SDK dependencies without `s3`. The TLS provider's AWS-LC cryptography crate is independent of S3 transport. See `dependency-graphs.log`.

The compile commands in `feature-compile-guards.log` verify O02's OLAP build without S3 and the explicit diagnostic for both invalid selections:

```sh
cargo check --locked --manifest-path server/Cargo.toml --no-default-features --features olap
cargo check --locked --manifest-path server/Cargo.toml --no-default-features --lib
cargo check --locked --manifest-path server/Cargo.toml --no-default-features --features olap,oltp --lib
```

The first command succeeded. The last two failed with `select exactly one engine: olap or oltp`. Additional errors in an invalid feature combination are retained in the log. OLTP runtime and storage execution evidence is recorded separately in `../../../oltp-evidence.md`.

`olap-no-s3-final.log` records the final warning-free locked OLAP check without S3 after the shared credentials accessor and `CatalogFence` import were gated to their users. It completed in 17.67 seconds. S3 does not implement OLTP backup in this checkpoint.

The later complete `python3.11 scripts/check_engine_features.py` run exited zero. `full-engine-matrix.log` and `full-engine-matrix-summary.json` record every valid selection's dependency graph, all library/integration tests, native version identity, and both invalid compile guards. This supersedes the earlier composite default test result with one clean run. The source-tree hash recorded at matrix completion matches the quiet process capture's source-tree hash.

| Selection | Test targets | Passed | Ignored | Native identity |
| --- | ---: | ---: | ---: | --- |
| Default OLAP with S3 | 39 | 759 | 1 live-S3 test | `nabedra-db 0.1.1 olap` |
| OLAP without S3 | 39 | 757 | 0 | `nabedra-db 0.1.1 olap` |
| OLTP without S3 | 7 | 97 | 1 crash child, invoked by its parent test | `nabedra-db 0.1.1 oltp` |
| OLTP with S3 | 7 | 97 | 1 crash child, invoked by its parent test | `nabedra-db 0.1.1 oltp` |

The full matrix used the environment below plus `RUST_TEST_THREADS=1`. Both OLTP selections include the four consensus integration fixtures. All nonignored assertions passed. These results establish local feature-selection compatibility; enabling S3 does not add an implemented OLTP backup path.

Historical default OLAP regression evidence combines the latest result for each target. That earlier evidence is not one clean full run. `default-initial.log` records 569 passing library assertions, one ignored live-S3 test, the integration targets through `startup`, and the old version-string expectation that failed after the intentional engine suffix was added. The earlier Namespace extraction compile error was corrected by using its existing accessor before this recorded run. `default-final-targets.log` records the corrected startup test and the storage/worker-pool targets that Cargo had not reached. Across the 39 targets, those latest results are 759 passed, zero failed, and one ignored. All original OLAP assertions remain; the version assertion now includes the selected engine.

```sh
cargo test --locked --manifest-path server/Cargo.toml --lib --tests
cargo test --locked --manifest-path server/Cargo.toml --test startup --test storage --test worker_pool
```

Both commands used `CARGO_BUILD_JOBS=2`, `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_PROFILE_TEST_DEBUG=0`, `CARGO_INCREMENTAL=0`, `TOKIO_WORKER_THREADS=2`, and the existing main checkout's `server/target` cache. The first recorded build took 2 minutes 47 seconds; the corrected target build took 3.99 seconds. These build times do not measure database performance.

`python-packaging.log` records 18 passing client, installer and CLI-management tests. O01/O21 decoder regressions accept counts above 100,000, preserve explicit query limits, and still reject booleans/negative counts. These are response-decoder and wire checks, not table-growth acceptance. Installer tests cover explicit engine/S3 selection, existing-prefix refusal and a mismatched compiled engine. CLI-management tests preserve the legacy version format while accepting the two recognized engine suffixes and rejecting unknown suffixes.

```sh
PYTHONPATH=orm/python/src:orm/python/tests python3.11 -m unittest test_cli_management test_install_nabedra test_client_api
```

`actual-binary-cli.json` records full SHA-256 values and native/Python version outputs from immutable copies of newly built OLAP and OLTP executables. The actual Python `nabedra --version --json` probe accepts both and retains its existing JSON keys and version value. No Cargo installation or image publication was performed.

The final OLTP checks used the same environment and shared target directory as the OLAP commands above:

```sh
cargo fmt --manifest-path server/Cargo.toml --all -- --check
cargo clippy --locked --manifest-path server/Cargo.toml --no-default-features --features oltp --all-targets -- -D warnings
cargo test --locked --manifest-path server/Cargo.toml --no-default-features --features oltp --lib
cargo test --locked --manifest-path server/Cargo.toml --no-default-features --features oltp --test oltp_replication -- --test-threads=1
```

Formatting passed. `oltp-clippy.log` records a warning-free Clippy run in 10.22 seconds. `oltp-final-lib.log` records 72 passing assertions in 12.38 seconds after a 20.28-second build. These include the official OpenRaft storage suite, bounded native ownership through cancellation, the modeled uncertain-write recovery latch, coherent purge-marker/tail reads, scalar wire round trips, and the exact CLI runtime's seven concurrent native execution threads. The lost-observation test models a fault boundary after a real synchronous write; it does not inject a native WAL failure.

The first default Clippy run matched the prior audit's 27 errors. `default-clippy-baseline.log`, `default-clippy.log`, and `default-clippy-comparison.json` preserve that comparison. After shared-module cleanup, `default-clippy-checkpoint.log` still exits with code 101 and 23 inherited errors. `default-clippy-checkpoint-comparison.json` records no added diagnostic classes/files and the removed shared findings. Default Clippy is not passing. The OLTP all-target checks above pass with warnings denied; unrelated default lint findings remain open.

`consensus-fixtures.log` records all four integration fixtures passing in 85.10 seconds after a 22.25-second build. They validate production admission checks for the prototype peer metadata; durable streamed log reads beyond the bounded full-read window; three actual mTLS Raft peers with separate RocksDB stores, majority failover, ReadIndex, stale-owner fencing and matching state snapshots; and a wide AppendEntries prefix whose acknowledgement covers only the entries sent, including conflicting-suffix replacement under a higher leader. These three peers run in one test process. They do not establish independent worker-process recovery, multiple groups per worker, multi-shard transactions, or the replicated all-ready RAM request queue.

The wide-write fixture admits six 6 MiB operations with the CLI-equivalent 512 MiB task budget. An earlier 64 MiB fixture budget correctly stopped state application with `operation exceeds task working memory`; `consensus-insufficient-budget.log` preserves that failure. The passing fixture uses a 5-second heartbeat and 15-to-20-second election interval to accommodate this debug build's local workload. These timers are test settings, not production latency guidance. The current replicated Operation command is a one-group dependency spike. Production transactions still require fixed prepared changes, compatible replica apply budgets, and the distributed acceptance work listed in the implementation status.
