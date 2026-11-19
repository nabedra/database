# Nabedra

Nabedra includes a Rust server and Python and TypeScript ORMs. The OLAP engine
stores immutable Parquet snapshots in object storage. The OLTP engine stores
durable indexed tables. The native Cache engine serves in-memory keys over RESP2.

## Repository layout

```text
server/          Rust database server, getting started and operational docs
orm/
  python/        Python ORM, CLI, application gateway and package tests
  typescript/    TypeScript ORM, CLI and package tests
scripts/         Repository installer and cross-component acceptance runners
docs/testing/    Shared test contracts, audits and recorded execution evidence
plans/           Design plans and implementation requirements
```

Start with the [server guide](server/README.md) to run the database. See the
[Python guide](orm/python/README.md) and the [TypeScript guide](orm/typescript/README.md)
for client examples. The [task tracker tutorial](server/TUTORIAL.md) walks
through the server and the Python client.

## Working in this repository

Each component keeps its setup instructions and documentation in its own folder.
Run repository commands from the root unless a guide says otherwise. Shared
acceptance scripts and test evidence cover interactions between the server and
clients.

Testing and release checks run locally through Codex or Claude Code agents with
explicit CPU and memory budgets. Use the reusable scripts in `scripts/` and the
contracts in `docs/testing/`. GitHub Actions are not used.

Read [CONTRIBUTION.md](CONTRIBUTION.md) before opening an issue or pull request,
and follow [AGENTS.md](AGENTS.md) when changing worker execution or recovery.

## Native Cache

Build `nabedra-db` with the `cache` Cargo feature for the RESP2-compatible
in-memory engine. The standalone and private cluster paths use the same
commands and Cache ORMs.

- [Standalone configuration and TLS](docs/cache/standalone.md)
- [Private EC2 deployment](docs/cache/ec2.md)
- [Benchmark reproduction](docs/testing/cache-benchmark.md)
