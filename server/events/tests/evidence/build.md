# Docker build and Rust verification

The Events compiler ran on Docker Engine 28.3.3 with Linux amd64 and Rust and Cargo 1.94.1. The builder was `rust:1.94.1-bookworm` at `sha256:6ae102bdbf528294bc79ad6e1fae682f6f7c2a6e6621506ba959f9685b308a55`. The compiler cgroup allowed 2 CPUs, 3 GiB of memory and zero extra swap. Cargo ran at most two jobs. After the Rust test completed, the compiler CPU limit dropped to 1 so the broker and OLAP fixtures could share the Docker VM.

The first runtime fixtures used a locked, unoptimized Rust build with debug symbols disabled. It linked the prebuilt Linux amd64 RocksDB 11.8.1 library, the version in the locked `librocksdb-sys` crate. The builder independently matched the package SHA-256 to the published checksum. The temporary fixture image copied the broker binary and shared libraries into the same Bookworm runtime layout as `deploy/Dockerfile`, with UID and GID 10001. The deployment Dockerfile builds a release binary and statically links RocksDB from its vendored C++ source. The final fixture binary below uses the vendored source and the unoptimized Rust profile. [Conda Forge RocksDB package](https://anaconda.org/conda-forge/rocksdb/files)

| Artifact | SHA-256 |
| --- | --- |
| `server/events/Cargo.lock` | `4beb7608a27af3080fec1a8fb848205c34849fc6d4ecfa2f73d8f45818154a4c` |
| `rocksdb-11.8.1-h5280e37_1_default.conda` | `00c1c04f682a110b5be390e407f855a08104dc24645c69c52114fdcd6ee58489` |
| Linked `nabedra-events` binary | `9a9b5ce80d04b2f76d7d811e462ca539181c2bc43d86fb4dd1bc5267277b98b1` |
| Local `nabedra-events:qa` image ID | `ae3fcc48c64f4b4d6cf550fe3952a4bbec08c881a0cd646cb15b8f2a185f8fd8` |

The shared build set `ROCKSDB_LIB_DIR` and `LD_LIBRARY_PATH` to the installed Conda library directory in the compiler container. It set `CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0` and `CARGO_PROFILE_TEST_DEBUG=0`. The test image resolved every dynamic library with `ldd`. Its default non-root entrypoint returned `nabedra-events 0.1.0` for `--version`.

| Check run inside Docker | Observed result |
| --- | --- |
| `cargo fmt --all -- --check` | Passed |
| `cargo check --locked --jobs 1 --all-targets` | Passed with pinned native headers and metadata-only checking |
| `cargo clippy --locked --jobs 1 --all-targets --no-deps -- -D warnings` | Passed with pinned native headers and metadata-only checking |
| `cargo tree --locked --prefix none` | 0 matches for database engine, Arrow, Parquet and AWS SDK crates |
| `cargo build --locked --jobs 2 --bin nabedra-events` | Linked the fixture binary against the checked RocksDB 11.8.1 shared library |
| `cargo test --locked --jobs 1 --lib -- --nocapture` | 1 passed, 0 failed, 0 ignored. The test took 0.51 seconds |

The RF1 test covers part of EV01 through EV03. It uses a real Raft group and RocksDB store. It sets the retained-byte counter to the partition limit without allocating 63 GiB, then checks an exact duplicate receipt, a mixed-batch rejection with the prior tail and deduplication state intact, and offset continuity after retention frees space. The Docker broker and OLAP acceptance reports record their own process-level assertions.

## Rebuild after the learner fence fix

The first broker growth run exposed a retired RF1 broker receiving a snapshot that still listed it as the sole voter. The replacement path now prepares a persisted snapshot that includes the exact committed membership, purges the preceding log and records a durable vote floor on the learner. The floor stops campaigning, vote RPCs and resumption of an obsolete local self-vote until a newer committed membership includes the replacement.

The locked shared-library build and `--version` check passed again with the revised source. It used the same builder, RocksDB package and Rust profile. The compiler used at most 2 CPUs for the link while the broker suite was paused, then returned to 1 CPU before restarting the runtime fixtures.

| Revised artifact | SHA-256 |
| --- | --- |
| Linked `nabedra-events` binary | `b684a76a68b2361a1bc120dd7e3acdf7abbc4770076a43678b1f7fcf540a7ba9` |
| Local `nabedra-events:qa-learner-fence` image ID | `630389e108220d346d14344f4e3a8bac02c303895e50f8d88b084ba7df76c944` |

Docker formatting and strict Clippy passed again on this source. The locked Rust library suite passed both tests in 2.30 seconds. The new regression created a real committed RF1 self-vote, reopened its disk with a replacement floor, and checked that neither the open nor the next restart could resume the old leadership or bootstrap an election. The retained-byte regression passed again.

## Source-built retirement and close fences

The final debug build used `cargo build --locked --jobs 2 --bin nabedra-events` in the same capped Docker compiler. Cargo built RocksDB 11.8.1 from the locked vendored C++ source and linked its archive into the binary. The packaged Bookworm image returned `nabedra-events 0.1.0` as UID and GID 10001, and `ldd` resolved its system libraries successfully. The final image uses the unoptimized Rust profile.

| Source-built artifact | SHA-256 |
| --- | --- |
| Linked `nabedra-events` binary | `f1242473f18608298f4e8f0dca4f1b8787592e81b347519cb7f9bcfeb68ae1b7` |
| Local `nabedra-events:qa-retirement-fence` image ID | `524b09a5e5aeade937ea25d9fd6772e2baafc16e13b4a933050ac25838040645` |

Retirement carries the quorum-confirmed exclusion cut and checks it against durable local membership and enrollment before deleting an old copy. The copy and its restart marker change together in one RocksDB batch. An accepted group open, close or retire transition holds the group-change lock through completion, including when the caller times out. Close seals and drains the old store before reuse. In-flight log, apply and snapshot jobs carry their storage permit inside the blocking disk operation.

The source-linked `cargo test --locked --jobs 1 --lib -- --nocapture` suite passed 4 tests in 2.94 seconds. It reran the retained-byte and stale RF1 self-vote checks, released a delayed retirement after real membership advanced and verified the original publish receipt, and canceled a storage waiter after its RocksDB job started. That last test checked that the detached job kept its permit and that the sealed store could no longer write into a reopened namespace. `cargo fmt --all -- --check` and strict all-target Clippy also passed on the source-built revision.

## Deployment configuration checks

`docker buildx build --builder desktop-linux --check -f server/events/deploy/Dockerfile server/events` completed without warnings. The definition check resolved the images and parsed the deployment Dockerfile.

An isolated Terraform 1.13.5 container initialized a temporary copy of the deployment files with `terraform init -backend=false -input=false` and installed `hashicorp/aws` 6.73.0. At 256 MiB the provider schema request was canceled. With the complete staging files, 0.25 CPU, 768 MiB of memory and networking disabled, `terraform validate` passed and the cgroup recorded zero OOM kills. The check covered the deployment configuration. It did not provision AWS resources.
