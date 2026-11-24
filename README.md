# Nabedra

Nabedra is a database server written in Rust, with ORMs for Python and
TypeScript. You pick one of three engines when you build the server:

- **OLAP** keeps each table as immutable Parquet snapshots in S3. It suits
  analytical queries.
- **OLTP** keeps indexed tables on a local disk, using `nabedra-kv`, our own
  storage engine. It suits many small reads and writes, and it doesn't need S3.
- **Cache** is an in-memory key-value store that speaks RESP2, the Redis wire
  protocol.

To run the database, start with the [server guide](server/README.md). For
client code, see the [Python guide](orm/python/README.md) or the
[TypeScript guide](orm/typescript/README.md). If you'd rather learn by
building something, the [task tracker tutorial](server/TUTORIAL.md) walks
through the server and the Python client.

## Repository layout

```text
server/          Rust database server, plus its setup and operations docs
  kv/            nabedra-kv, the OLTP storage engine
  events/        Nabedra Events, the replicated event log
orm/
  python/        Python ORM, the nabedra CLI and the application gateway
  typescript/    TypeScript ORM and CLI
  cache-contract/  Behavior both Cache clients must share, with its test runner
scripts/         Installer and acceptance runners that span components
docs/testing/    Test contracts, audits and recorded test runs
plans/           Design plans and requirements
```

Each component documents its own setup in its folder. Run commands from the
repository root unless a guide says otherwise.

## Cache

Build `nabedra-db` with the `cache` Cargo feature to get the Cache engine.
Standalone servers and private clusters accept the same commands, and the same
ORM code works against both.

- [Standalone setup and TLS](docs/cache/standalone.md)
- [Private cluster on EC2](docs/cache/ec2.md)
- [Reproducing the benchmarks](docs/testing/cache-benchmark.md)

## Testing

Focused checks run locally, usually through Codex or Claude Code agents with
fixed CPU and memory limits. They use the scripts in `scripts/` and the
contracts in `docs/testing/`. GitHub Actions runs the full Rust builds and test
suites on every push to `main` and every PR, and maintainers can also start the
Docker image, crash campaign and acceptance jobs there.

## Contributing

We only accept bug fixes from outside contributors. Read
[CONTRIBUTING.md](CONTRIBUTING.md) before you open an issue or a pull request.

## License

Nabedra is available under the [MIT License](LICENSE).
