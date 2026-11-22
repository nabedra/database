# Nabedra Database server

`nabedra-db` is the Nabedra server. It serves an HTTP API, or HTTPS when you
give it a certificate. The default build uses the OLAP engine, which stores
every table as immutable Parquet snapshots in S3. For local use, `serve` runs
an API and a worker in one process. A cluster runs them as separate processes.

## Get started

You need Rust 1.94.1 or newer, an S3 bucket that already exists, and AWS
credentials that can read objects under your database prefix and write them
with conditional writes. The server never creates buckets.

Install the published server:

```sh
cargo install nabedra-database --version 0.1.1 --locked
nabedra-db --version
```

Then pick a prefix for the new database, choose a private bearer token, and
start the server:

```sh
export AWS_PROFILE='<configured-profile>'
export AWS_REGION='<bucket-region>'
export NABEDRA_S3_BUCKET='<existing-bucket>'
export NABEDRA_S3_PREFIX='nabedra-v0/'
export NABEDRA_TOKEN='<your-private-bearer-token>'
nabedra-db serve --host 127.0.0.1 --port 8000
```

The AWS SDK also picks up credentials from environment variables or an
instance role, in which case you can leave out `AWS_PROFILE`. The server
doesn't read `.env` files.

Run only one `serve` process per bucket and prefix. If you need several APIs or
workers, follow the
[cluster deployment reference](https://github.com/zPy52/nabedra-database/blob/main/server/docs/reference.md#cluster-process-acceptance)
instead.

Check that the server is ready from another terminal:

```sh
curl --fail --silent --show-error http://127.0.0.1:8000/health/ready
```

Now connect with the [Python ORM](https://github.com/zPy52/nabedra-database/blob/main/orm/python/README.md),
using the same token. Clients only need the server URL and the token. AWS
credentials stay on the server. To serve HTTPS, add
`--tls-cert /path/to/cert.pem --tls-key /path/to/key.pem`.

Press Ctrl-C to stop. The server finishes the work it has already admitted
before it exits. Once every worker has stopped, restart the same database with
`--cold-start`. That flag opens the committed data with a new, empty request
queue and abandons any requests that were still outstanding. Read the
[configuration reference](https://github.com/zPy52/nabedra-database/blob/main/server/docs/reference.md#configuration)
before you open a catalog written by an older version or deploy a cluster.

## Develop from this checkout

From the repository root:

```sh
CARGO_BUILD_JOBS=2 cargo build --locked --manifest-path server/Cargo.toml
server/target/debug/nabedra-db serve --host 127.0.0.1 --port 8000
```

This uses the same S3 environment variables as above. To install the
checkout's Python CLI and ORM together with a release build of the server, you
also need Python 3.11 or newer:

```sh
python3.11 scripts/install_nabedra.py --prefix .nabedra
.nabedra/bin/nabedra --version --json
```

The [CLI and application gateway guide](https://github.com/zPy52/nabedra-database/blob/main/server/docs/cli-and-serverless.md)
covers the installed commands, configuration files and shared compute.

## Choose an engine

The checkout builds exactly one engine, selected with the `olap`, `oltp` or
`cache` Cargo feature. Any combination of them fails to compile. The default
features are still `olap,s3`, so for another engine you turn the defaults off
and name the one you want:

```sh
cargo build --locked --manifest-path server/Cargo.toml --no-default-features --features olap
cargo build --locked --manifest-path server/Cargo.toml --no-default-features --features oltp
```

The Cache engine has its own guides, linked from the
[repository README](https://github.com/zPy52/nabedra-database/blob/main/README.md#cache).
It can't be built with `s3`.

The published 0.1.1 release predates this split. `nabedra-db --version` tells
you which engine a binary was built with.

### OLTP

The OLTP engine stores data on a local or attached disk using `nabedra-kv`,
the storage engine in
[`kv/`](https://github.com/zPy52/nabedra-database/tree/main/server/kv). Its
design is in the
[native storage engine plan](https://github.com/zPy52/nabedra-database/blob/main/plans/native-storage-engine.md).
OLTP doesn't need S3 or AWS credentials. Building it needs Rust and a C
compiler, which `ring` (the TLS crypto library) uses. You don't need a C++
compiler, CMake or libclang.

OLTP data uses pool format 2, which keeps the store under `<pool>/kv`. Data
directories from releases before `nabedra-kv` (format 1, or RocksDB files)
won't open. The server refuses them with an error that names the path and
leaves them unchanged, and there is no in-place migration, so start those
workers on empty volumes.

A standalone OLTP server has one worker and one data directory:

```sh
mkdir -p /path/to/attached-volume/nabedra
export NABEDRA_TOKEN='<your-private-bearer-token>'
server/target/debug/nabedra-db serve --data-dir /path/to/attached-volume/nabedra
python3.11 scripts/install_nabedra.py --prefix .nabedra-oltp --engine oltp
```

For replication, run registered workers that host replicated OLTP groups behind
a remote gateway. Adding the `s3` feature to an `oltp` build only pulls in the
S3 transport dependencies. It doesn't add backups, and it doesn't turn on the
OLAP engine.

The installer includes AWS dependencies for OLAP by default and leaves them out
for OLTP. Pass `--with-s3` or `--without-s3` to override either default. Before
it creates the install prefix, the installer checks which engine the built
binary contains. It also refuses to reuse an existing prefix, so installing a
different engine can't quietly replace an existing install.

### Scoped queues on registered RF3 workers

Scoped queues are off by default. Roll out the upgrade in this order. First
upgrade every Control voter, and only then let processes advertise the
capability. Before you activate it for a database, upgrade the Data, Catalog,
Timestamp and Status replicas, along with the recovery and restore tooling.

1. Set `node.scoped_oltp` to `true` in each registered worker's bootstrap
   config. Data-only workers use the top-level `scoped_oltp` setting.
2. Set `topology.scoped_oltp` to `true` in each remote gateway config.
3. On a registered worker's admin endpoint, call `queue.scoped.status` with an
   empty payload. Then call `queue.scoped.admission` with the returned
   capability as `expected` and `enabled` set to `true`. Control checks that
   the capability and membership are still current before it starts admitting
   requests.

To pause new scoped admissions, submit the latest capability with `enabled`
set to `false`. Leave scoped support on in the process configs while requests
that were already admitted finish under their recorded policy. After a full
stop, you have to activate again. The restart abandons pending work but keeps
every durable decision.

Once scoped admission is active, eligible primary-key `Get` and `Update`
requests go through the RF3 queue of the Data range that owns the key. Every
other operation keeps its old execution path, and so do requests that were
accepted under the earlier all-ready replication policy. The
[OLTP contract](https://github.com/zPy52/nabedra-database/blob/main/docs/testing/oltp-contract.md)
defines what acceptance, recovery and receipts guarantee on this path.

### Direct point operations (`data.direct.v1`)

Every worker and gateway advertises the `data.direct.v1` Control capability
when it starts. Control activates the capability once every enrolled worker and
API ingress has advertised its current identity. Activation is durable and is
never reverted.

- Upgrade every Control voter first. As with scoped queues, an older Control
  replica can't decode the advertisement.
- Until activation, no node proposes a direct Data entry, and Data groups
  reject any that arrive.
- After activation, a worker that hasn't advertised can't join the ready set
  or become a voter or learner in any group. Control returns
  `DirectCapabilityRequired`. This covers older builds and re-enrolled
  processes that haven't advertised yet.
- You can't downgrade after activation. By then the Data groups hold direct
  entries, dedup records and forward markers, and an older build can't read
  the activated group configuration.
- `execution.direct_oltp` on workers and `topology.direct_oltp` on remote
  gateways default to `true`. Setting either to `false` keeps the capability
  active but sends eligible operations through the queue. The request ID is
  still settled at its anchor range before the request is queued.
- While the capability is active, the server refuses incremental backups. Take
  full backups instead.

See [OLTP worker recovery and direct-mode rollback](../docs/oltp-recovery.md)
for retained-mount restart, lost-volume replacement, and full archive restore.

### Docker

The Docker build takes the same engine choice. The OLAP image keeps the Python
CLI as its entrypoint, while the OLTP image runs `nabedra-db` directly:

```sh
docker build --build-arg NABEDRA_ENGINE=oltp -t nabedra:oltp .
docker run --rm -e NABEDRA_TOKEN -v /path/to/attached-volume:/data nabedra:oltp serve --data-dir /data --host 0.0.0.0
```

The `NABEDRA_S3` build argument defaults to `auto`, which includes S3 for OLAP
and leaves it out for OLTP. Set it to `true` or `false` to override that. Put
the engine name in release image and artifact tags, so that switching engines
in a deployment is always a visible change.

### Checking the feature matrix

`python3.11 scripts/check_engine_features.py` runs scenario O02. For each
valid feature set (the defaults, `olap`, `oltp`, `oltp-backup-s3` and `cache`)
it checks the dependency graph, builds and tests the server, and confirms the
engine the binary reports. It also confirms that every invalid combination
fails with the expected compile error. Pass
`--dependencies-only` to inspect the dependency graphs alone. That mode
doesn't build anything, so it can't show that the binaries work. Don't use
`--all-features` here, since the engines can't be built together.

## More documentation

- [Task tracker tutorial](https://github.com/zPy52/nabedra-database/blob/main/server/TUTORIAL.md)
- [Configuration, HTTP API and storage reference](https://github.com/zPy52/nabedra-database/blob/main/server/docs/reference.md)
- [Overview, limits and recorded benchmarks](https://github.com/zPy52/nabedra-database/blob/main/server/docs/overview.md)
- [Scheduler test contract](https://github.com/zPy52/nabedra-database/blob/main/docs/testing/scheduler-contract.md)
  and its [recorded results](https://github.com/zPy52/nabedra-database/blob/main/docs/testing/scheduler-evidence.md)
- [Execution and recovery design](https://github.com/zPy52/nabedra-database/blob/main/plans/resource-aware-scheduling.md)

Before you send a server change, run the formatting, Clippy and test commands
in the [contribution guide](https://github.com/zPy52/nabedra-database/blob/main/CONTRIBUTING.md#development-checks).
