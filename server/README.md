# Nabedra Database server

The Rust server stores tables as immutable Parquet snapshots in object storage
and exposes an HTTP or native HTTPS API. Local `serve` runs an API and worker
together. Cluster deployments run separate API and worker processes.

## Get started

You need Rust 1.94.1 or newer with Cargo, an existing S3 bucket, and an AWS
identity allowed to read and conditionally write objects under your database
prefix. The server does not create buckets.

Install the published native server:

```sh
cargo install nabedra-database --version 0.1.1 --locked
nabedra-db --version
```

Configure a new database prefix and a private bearer token in your shell:

```sh
export AWS_PROFILE='<configured-profile>'
export AWS_REGION='<bucket-region>'
export NABEDRA_S3_BUCKET='<existing-bucket>'
export NABEDRA_S3_PREFIX='nabedra-v0/'
export NABEDRA_TOKEN='<your-private-bearer-token>'
nabedra-db serve --host 127.0.0.1 --port 8000
```

The AWS SDK also accepts environment credentials and roles. Omit `AWS_PROFILE`
when using those. The server does not load `.env`. Run one standalone `serve`
process per bucket/prefix. For multiple APIs or workers, follow the
[cluster deployment reference](https://github.com/zPy52/nabedra-database/blob/main/server/docs/reference.md#cluster-process-acceptance).

In another terminal, check readiness:

```sh
curl --fail --silent --show-error http://127.0.0.1:8000/health/ready
```

Connect using the [Python ORM](https://github.com/zPy52/nabedra-database/blob/main/orm/python/README.md) with the same bearer
token. Clients need the server URL and token; AWS credentials stay on the server.
Add `--tls-cert /path/to/cert.pem --tls-key /path/to/key.pem` for native HTTPS.

Stop with Ctrl-C to drain admitted work. After all workers stop, restart the
same database with `--cold-start`. This opens committed data with a new empty
request queue and abandons outstanding requests. See the
[configuration reference](https://github.com/zPy52/nabedra-database/blob/main/server/docs/reference.md#configuration) before opening an
older catalog or deploying a cluster.

## Develop from this checkout

Run these commands from the repository root:

```sh
CARGO_BUILD_JOBS=2 cargo build --locked --manifest-path server/Cargo.toml
server/target/debug/nabedra-db serve --host 127.0.0.1 --port 8000
```

Use the S3 environment configuration above. To install the checkout's Python CLI
and ORM together with a release server build, you also need Python 3.11+:

```sh
python3.11 scripts/install_nabedra.py --prefix .nabedra
.nabedra/bin/nabedra --version --json
```

The [CLI and application gateway guide](https://github.com/zPy52/nabedra-database/blob/main/server/docs/cli-and-serverless.md) describes
installed commands, configuration files and shared compute.

## Choose a build engine

This checkout has mutually exclusive `olap` and `oltp` Cargo features. The
published 0.1.1 release predates this choice. Defaults remain `olap,s3`.
Select exactly one engine when building without defaults:

```sh
cargo build --locked --manifest-path server/Cargo.toml --no-default-features --features olap
cargo build --locked --manifest-path server/Cargo.toml --no-default-features --features oltp
```

OLTP uses RocksDB on an attached filesystem and does not need an S3 bucket or
AWS credentials. Native compilation needs a C++ compiler, CMake and libclang.
On Debian, install `build-essential cmake clang libclang-dev`; on macOS, install
the Xcode command line tools and make libclang available to bindgen. The lockfile
pins RocksDB's Rust binding to 0.25.0 and its native library to 11.8.1.
`nabedra-db --version` identifies the compiled engine.

The current OLTP topology is one standalone worker and one data directory.
Distributed consensus, multi-worker transactions, automatic shard/volume growth,
and S3 backup are still required by [the implementation plan](../plans/oltp-engine.md).
Enabling `s3` with `oltp` only includes the optional transport dependencies; it
does not implement a backup or enable the OLAP engine.

```sh
mkdir -p /path/to/attached-volume/nabedra
export NABEDRA_TOKEN='<your-private-bearer-token>'
server/target/debug/nabedra-db serve --data-dir /path/to/attached-volume/nabedra
python3.11 scripts/install_nabedra.py --prefix .nabedra-oltp --engine oltp
```

The installer keeps OLAP's S3 default and omits AWS dependencies for OLTP unless
`--with-s3` is explicit. `--without-s3` also works for OLAP. It verifies the built
binary's engine before creating the installation prefix. Existing prefixes are
refused so selecting another engine cannot silently replace an installation.

Docker uses the same choices. The OLAP image retains its existing Python CLI
entrypoint; the OLTP image invokes `nabedra-db` directly:

```sh
docker build --build-arg NABEDRA_ENGINE=oltp -t nabedra:oltp .
docker run --rm -e NABEDRA_TOKEN -v /path/to/attached-volume:/data nabedra:oltp serve --data-dir /data --host 0.0.0.0
```

`NABEDRA_S3=auto` defaults to S3 for OLAP and no S3 for OLTP. Set it to `true`
or `false` to override that dependency choice. Tag release images/artifacts with
the engine so deployment changes remain explicit.

Run `python3.11 scripts/check_engine_features.py` for O02's four dependency,
build/test and binary-identity checks plus both invalid feature combinations.
Use `--dependencies-only` when inspecting graphs; that option does not execute
builds or establish runtime acceptance. Never use `--all-features` for this
mutually exclusive engine matrix.

## More documentation

- [Task tracker tutorial](https://github.com/zPy52/nabedra-database/blob/main/server/TUTORIAL.md)
- [Configuration, HTTP API and storage reference](https://github.com/zPy52/nabedra-database/blob/main/server/docs/reference.md)
- [Database overview, limits and recorded benchmarks](https://github.com/zPy52/nabedra-database/blob/main/server/docs/overview.md)
- [Shared test contract](https://github.com/zPy52/nabedra-database/blob/main/docs/testing/scheduler-contract.md) and
  [execution evidence](https://github.com/zPy52/nabedra-database/blob/main/docs/testing/scheduler-evidence.md)
- [Execution and recovery design](https://github.com/zPy52/nabedra-database/blob/main/plans/resource-aware-scheduling.md)

The reference documents retain the existing detailed material. For local server
checks, run Cargo formatting, Clippy and tests as described in the
[contribution guide](https://github.com/zPy52/nabedra-database/blob/main/CONTRIBUTION.md#development-checks).
