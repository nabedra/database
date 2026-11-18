# Nabedra

Nabedra is a monorepo for the database server and its language clients. The
database stores immutable Parquet snapshots in object storage and exposes an
HTTP API. The Rust server and synchronous Python ORM are the current components.

## Repository layout

```text
server/          Rust database server, getting started and operational docs
orm/
  python/        Python ORM, CLI, application gateway and package tests
scripts/         Repository installer and cross-component acceptance runners
docs/testing/    Shared test contracts, audits and recorded execution evidence
plans/           Design plans and implementation requirements
```

Start with the [server guide](server/README.md) to run the database, then the
[Python guide](orm/python/README.md) to connect and query it. The
[task tracker tutorial](server/TUTORIAL.md) walks through both components.

Python is the only implemented ORM language. Future clients may include
TypeScript/JavaScript, Java/Kotlin, Go or Rust, each under `orm/<language>/`.

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
