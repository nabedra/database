# Nabedra Database server rename evidence

Validation ran on 2026-11-16 on macOS, x86_64, with Cargo/Rust 1.99.1. The checkout contains the rename from the `meshdb-server` package to `nabedra-database-server`. No dependencies changed in `Cargo.lock`.

## Executed Rust assertions

The final production source passed 567 library tests and 188 integration tests across 35 integration targets, for 755 passing tests, with no ignored tests. Library assertions reported 55.57 seconds. Integration assertions reported 56.89 seconds summed across targets. These are test durations, not throughput benchmarks.

All commands used `CARGO_TARGET_DIR="$SHARED_CARGO_TARGET"`, where the variable names an existing generated-artifact cache outside this checkout. Original project source files were not changed. Test builds used:

```sh
export CARGO_TARGET_DIR="$SHARED_CARGO_TARGET"
export CARGO_BUILD_JOBS=1 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --lib --tests -- --test-threads=2
cargo test --manifest-path server/Cargo.toml --locked --offline --no-default-features --test '*' -- --test-threads=2
```

The first listed command's final library run passed all 567 tests, then stopped at one changed integration fixture's stale expected fingerprint. The second command reran every integration target after that fixture correction and passed all 188 tests. No production source changed between these runs. The library build reported 1 minute 42 seconds; the integration build reported 4.49 seconds. Logs are `/tmp/nabedra-rename-rust-tests-nodefault-library.log` and `/tmp/nabedra-rename-rust-tests-nodefault-integration.log`.

An earlier library run exposed seven stale authentication known-answer fixtures after the peer signing domains changed. Those synthetic Ed25519 signatures and the API signing key were independently recomputed with Python `cryptography` and stdlib HMAC/SHA256. HKDF uses the existing cluster UUID salt and admin-token input with the new `nabedra.api-signing.v1` info. The new API public-key fixture is `ec08a2e3c989ad1713ff26c02d71342c46b11dc83c777f37a6df0501462982e1`. Existing signature rejection assertions remain intact. The changed template fixture's new expected fingerprint was independently computed from canonical JSON and the unchanged persisted domain.

The [scheduler test contract](../docs/testing/scheduler-contract.md) remains in force. Executed assertions include Q03 replication-before-acceptance, Q04/Q06 receipt observation without resubmission, R04 cold-run abandonment, C07 control capacity and reservation settlement, T04/T05 durable decisions across cold fencing, U01 local queue integration and U03 seeded conservation. These unit/integration assertions do not establish every process, benchmark or provider scenario in that contract. The rename makes no scheduling, admission or mutation-publication behavior change.

`cargo fmt --manifest-path server/Cargo.toml --check` and `git diff --check -- server Dockerfile` passed.

## Durable identity compatibility

The new `existing_storage_identity_and_template_fingerprints_survive_rename` regression passed with fixed pre-rename inputs and expected digests:

| Identity | Preserved expected digest |
| --- | --- |
| Storage key for `tenant-data`, `us-east-1`, `meshdb-v0/` | `a31a2977bd480b802a05931f659ab9bc99e1443a6ff65d756741b15dfca545c4` |
| Original outgoing template using namespace `meshdb/` | `a6f1bc932125f814fed2b0676c9f3350e6bd4eda9d570efb5abd17651c0f53d1` |

Storage identity, configuration/template fingerprint and persisted worker verifier domains retain their original `meshdb.*` values. The stored callback adapter tag remains `meshdb.callbacks.v1`. Existing canonical fingerprint and worker verifier vectors also passed. Catalog/snapshot formats are unchanged. Existing configured namespaces continue to select the same objects; previous legacy default data requires explicit `NABEDRA_S3_PREFIX=meshdb-v0/` because the new default is `nabedra-v0/`.

Public and peer headers, ephemeral authentication domains, environment variables, temporary paths, worker thread names, crate imports and executable names use Nabedra. Restart peers with matching renamed versions. `X-Nabedra-Database` remains a Python gateway selector; direct Rust storage-context validation keeps its existing behavior.

## Build and scope

The default-feature release build passed in 8 minutes 10 seconds, with no warnings. Its command was:

```sh
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 cargo build --manifest-path server/Cargo.toml --locked --offline --release --bin nabedra-database-server
```

The resulting 44 MiB executable is `$SHARED_CARGO_TARGET/release/nabedra-database-server`. `--version` returned `nabedra-database-server 0.1.0`, and `--help` returned the renamed command/help text successfully. Its SHA256 is `2790afac37bf79561b239742cb39a47ba7be4204c6289e8a9d141cd464c7acb4`. The build log is `/tmp/nabedra-rename-rust-release.log`.

The Dockerfile now builds and installs `nabedra-database-server`, uses the `nabedra` user/home, and launches `nabedra database serve --config /etc/nabedra/nabedra-database.json`. A Docker image build, real AWS storage acceptance and Kubernetes lifecycle acceptance were not executed for this rename. Historical reports under [notes](notes/README.md) retain their original names and command records.

Initial builds encountered the host's low disk space. An initial debug-info build was interrupted and its generated artifacts removed; the cached no-debug profile was then used. An intermediate test link failed with ENOSPC before its assertions ran. Obsolete generated package artifacts were removed and the recorded final assertions were executed successfully. The prior `meshdb-server` executable and dependency caches were retained.
