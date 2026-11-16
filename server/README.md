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
